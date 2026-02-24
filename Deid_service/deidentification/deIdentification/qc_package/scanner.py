import random
from django.db import transaction
from deIdentification.settings import CREATE_SAVEPOINT_IN_TRANSACTION
from collections import defaultdict
from sqlalchemy import create_engine, Table, MetaData, select, or_
from sqlalchemy.engine import Engine
from django.conf import settings
import traceback
import pandas as pd
import numpy as np

from nd_api.schemas.table_config import TableDetailsForUI
from qc_package.generator import DataGenerator
from qc_package.builders import DectorMapping, Detector, UnstructuredDetector
from qc_package.schema import OutputSchemaForTable, FinalQCResult, ColumnQCResult
from deIdentification.nd_logger import nd_logger
from nd_api_v2.models import SchedulerConfig
from nd_api_v2.models.configs import get_mapping_db_connection_str,get_pii_config,get_secondary_pii_configs

from core.dbPkg.mapping_loader import (
    PATIENT_MAPPING_TABLE,
    ENCOUNTER_MAPPING_TABLE,
    ENCOUNTER_MAPPING_TABLE_ENCID_COL,
)
from core.dbPkg.dbhandler import NDDBHandler
from core.dbPkg.pii_loader import PII_TABLE_NAME, PII_ND_PATIENT_ID_COLUMN
from nd_api_v2.models import IgnoreRowsDeIdentificaiton, Table as TableModel, Status
from worker.models import Task, Chain
from qc_package.builders.base import QCErrors
from portal.alerts import alert_sender



def convert_numpy_types_to_python(obj):
    """
    Recursively convert numpy/pandas types to native Python types for JSON serialization.
    """
    if isinstance(obj, dict):
        return {key: convert_numpy_types_to_python(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types_to_python(item) for item in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif hasattr(obj, 'item'):  # For pandas scalar types
        return obj.item()
    else:
        return obj



def merge_dicts_list(dicts_list: list[dict]):
    merged = defaultdict(list)

    for d in dicts_list:
        for key, values in d.items():
            merged[key].extend(values)

    for k, v in merged.items():
        seen = set()
        merged[k] = [x for x in v if not (x in seen or seen.add(x))]

    return dict(merged)

def get_patient_identifiers(table_config: TableDetailsForUI):
    identifiers = {"patient_id": [], "encounter_id": [], "appointment_id": []}
    for col_conf in table_config["columns_details"]:
        if not col_conf["is_phi"]:
            continue
        if col_conf['de_identification_rule'].startswith("PATIENT_"):
            identifiers["patient_id"].append(col_conf['column_name'])
        elif col_conf['de_identification_rule'] == "ENCOUNTER_ID":
            identifiers["encounter_id"].append(col_conf['column_name'])
        elif col_conf['de_identification_rule'] == "APPOINTMENT_ID":
            identifiers["appointment_id"].append(col_conf['column_name'])
    return identifiers

class LoadMappingData:
    @classmethod
    def load(cls, source_df: pd.DataFrame, table_obj: TableModel):
        nd_logger.info(f"[LoadMappingData] Loading mapping data for table: {table_obj.metadata.table_name}")
        available_identifiers = {}
        end_ndids = []
        for column_conf in table_obj.metadata.table_details_for_ui["columns_details"]:
            if column_conf["is_phi"] and column_conf[
                "de_identification_rule"
            ].startswith("PATIENT_"):
                patient_identifier = column_conf["de_identification_rule"].split(
                    "PATIENT_"
                )[1].lower()
                if patient_identifier not in available_identifiers:
                    available_identifiers[patient_identifier] = []
                available_identifiers[patient_identifier].extend(
                    source_df[column_conf["column_name"]].tolist()
                )
            elif (
                column_conf["is_phi"]
                and column_conf["de_identification_rule"] == "ENCOUNTER_ID"
            ):
                end_ndids.extend(source_df[column_conf["column_name"]].tolist())

        # --- Load Patient-Mapping Data ---
        m_conn_str = get_mapping_db_connection_str()
        nd_logger.info(f"[LoadMappingData] Connecting to mapping DB with: {m_conn_str}")
        engine: Engine = create_engine(m_conn_str)
        metadata = MetaData()
        patient_mapping_table = Table(
            PATIENT_MAPPING_TABLE, metadata, autoload_with=engine
        )

        with engine.connect() as conn:
            conditions = []
            for col_name, values in available_identifiers.items():
                values = [v for v in values if pd.notnull(v)]
                column = getattr(patient_mapping_table.c, col_name)
                if isinstance(values, (list, tuple, set)):
                    conditions.append(column.in_(values))
                else:
                    conditions.append(column == values)
            query = select(patient_mapping_table).where(or_(*conditions))
            nd_logger.debug(f"[LoadMappingData] Executing query for patient mapping: {query}")
            patient_mapping_df = pd.read_sql(query, conn)

        # --- Load Encounter-Mapping Data ---
        engine: Engine = create_engine(m_conn_str)
        metadata = MetaData()
        encounter_mapping_table = Table(
            ENCOUNTER_MAPPING_TABLE, metadata, autoload_with=engine
        )
        with engine.connect() as conn:
            end_ndids = [v for v in end_ndids if pd.notnull(v)]
            nd_logger.debug(f"[LoadMappingData] Encounter mapping end_ndids: {end_ndids}")
            query = select(encounter_mapping_table).where(
                encounter_mapping_table.c[ENCOUNTER_MAPPING_TABLE_ENCID_COL].in_(end_ndids)
            )
            nd_logger.debug(f"[LoadMappingData] Executing query for encounter mapping: {query}")
            encounter_mapping_df = pd.read_sql(query, conn)

        nd_logger.info(f"[LoadMappingData] Loaded patient_mapping_df shape: {patient_mapping_df.shape}")
        nd_logger.info(f"[LoadMappingData] Loaded encounter_mapping_df shape: {encounter_mapping_df.shape}")

        return patient_mapping_df, encounter_mapping_df


class LoadPIIData:
    @classmethod
    def load(cls, patient_mapping_df:pd.DataFrame, encounter_mapping_df: pd.DataFrame, table_obj: TableModel):
        nd_logger.info(f"[LoadPIIData] Loading PII data for table: {table_obj.metadata.table_name}")
        def get_all_ndids():
            series_list = []

            if not patient_mapping_df["nd_patient_id"].dropna().empty:
                series_list.append(patient_mapping_df["nd_patient_id"].dropna())

            if not encounter_mapping_df["nd_patient_id"].dropna().empty:
                series_list.append(encounter_mapping_df["nd_patient_id"].dropna())
            if series_list:
                all_nd_df = pd.concat(series_list).unique()
            else:
                all_nd_df = pd.Series([], dtype=object)
            all_ndids = all_nd_df.tolist()
            return all_ndids

        all_ndids = get_all_ndids()
        nd_logger.debug(f"[LoadPIIData] all_ndids (nd_patient_ids) count: {len(all_ndids)}")
        if len(all_ndids)<1:
            nd_logger.warning(f"[LoadPIIData] No nd_patient_ids found for PII loading: returning empty frames")
            return {"pii_df": pd.DataFrame(columns=["nd_patient_id"]), "secondary_dfs": [], "global_df": []}
        # --- Load PII Data ---
        pii_config = get_pii_config()
        p_conn_str = pii_config["connection_str"]

        engine: Engine = create_engine(p_conn_str)
        metadata = MetaData()
        pii_table = Table(PII_TABLE_NAME, metadata, autoload_with=engine)

        with engine.connect() as conn:
            query = select(pii_table).where(pii_table.c[PII_ND_PATIENT_ID_COLUMN].in_(all_ndids))
            nd_logger.debug(f"[LoadPIIData] Executing query for pii_table: {query}")
            pii_df = pd.read_sql(query, conn)

        nd_logger.info(f"[LoadPIIData] Loaded pii_df shape: {pii_df.shape}")

        # --- Load Secondary PII Data ---
        secondary_dfs = []
        secondary_config = get_secondary_pii_configs()
        s_conn_str = secondary_config["connection_str"]
        s_engine: Engine = create_engine(s_conn_str)
        s_metadata = MetaData()

        with s_engine.connect() as conn:
            for table_conf in secondary_config["tables_config"]:
                table_name = table_conf["table_name"]
                sec_table = Table(table_name, s_metadata, autoload_with=s_engine)
                query = select(sec_table).where(sec_table.c[PII_ND_PATIENT_ID_COLUMN].in_(all_ndids))
                nd_logger.debug(f"[LoadPIIData] Executing query for secondary PII table {table_name}: {query}")
                df = pd.read_sql(query, conn)
                nd_logger.info(f"[LoadPIIData] Loaded secondary PII df for '{table_name}' - shape: {df.shape}")
                secondary_dfs.append(df)

        pii_data = {
            "pii_df": pii_df,
            "secondary_dfs": secondary_dfs,
            "global_df": pd.DataFrame(),
        }

        return pii_data


class DbScanner:
    def __init__(self):
        pass

    @classmethod
    def get_important_columns(cls, table_config: TableDetailsForUI):
        important_cols = []
        for col_conf in table_config["columns_details"]:
            if col_conf["is_phi"]:
                important_cols.append(col_conf["column_name"])
        nd_logger.debug(f"[DbScanner] Important columns: {important_cols}")
        return important_cols

    @classmethod
    def get_structured_detectors(
        cls,
        source_df: pd.DataFrame,
        table_obj: TableModel,
        qc_config: dict,
    ) -> tuple[list[tuple[str, Detector]], dict]:
        nd_logger.info(f"[DbScanner] Getting structured detectors for table: {table_obj.metadata.table_name}")
        detectors = []
        patient_mapping_df, encounter_mapping_df = LoadMappingData.load(
            source_df, table_obj
        )
        for col_conf in table_obj.metadata.table_details_for_ui["columns_details"]:
            if col_conf["is_phi"] and col_conf["de_identification_rule"] not in [
                "NOTES",
                "GENERIC_NOTES",
            ]:
                if col_conf["de_identification_rule"].startswith("PATIENT_"):
                    detector_cls: Detector = DectorMapping["PATIENT_ID"]
                else:
                    detector_cls: Detector = DectorMapping[
                        col_conf["de_identification_rule"]
                    ]

                patient_identifiers = get_patient_identifiers(table_obj.metadata.table_details_for_ui)
                detector_obj = detector_cls(
                    patient_mapping_df=patient_mapping_df,
                    encounter_mapping_df=encounter_mapping_df,
                    appointment_mapping_df = pd.DataFrame(),
                    qc_config=qc_config,
                    column_config=col_conf,
                    patient_id_cols=patient_identifiers["patient_id"],
                    encounter_id_cols=patient_identifiers["encounter_id"],
                    appointment_id_cols=patient_identifiers["appointment_id"]
                )
                nd_logger.debug(f"[DbScanner] Created detector for column '{col_conf['column_name']}': {detector_cls}")
                detectors.append((col_conf["column_name"], detector_obj))
        nd_logger.info(f"[DbScanner] Total detectors created: {len(detectors)}")
        return detectors, patient_mapping_df, encounter_mapping_df

    def scan_table(
        self,
        table_id: int,
        read_limit: int = 50000,
    ) -> OutputSchemaForTable:
        nd_logger.info(f"[DbScanner] Beginning scan_table for table_id: {table_id}")
        table_obj = TableModel.objects.get(id=table_id)
        important_cols = DbScanner.get_important_columns(table_obj.metadata.table_details_for_ui)
        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")
        src_conn_str = scheduler_config.get_source_connection_str()
        dest_conn_str = scheduler_config.get_deid_connection_str()
        qc_config = table_obj.get_qc_config()
        try:
            with transaction.atomic(savepoint=CREATE_SAVEPOINT_IN_TRANSACTION):
                chain, created = Chain.all_objects.get_or_create(
                    reference_uuid=table_obj.get_qc_chain_reference_uuid()
                )
                nd_logger.info(f"[DbScanner] Created Chain (id={chain.id}, created={created}) for QC on table {table_obj.metadata.table_name}")
                if not created:
                    chain.revive_and_save()
                    nd_logger.info(f"[DbScanner] Revived pre-existing Chain for table {table_obj.metadata.table_name}")
                qc_tasks_list = []
                for start_value in range(table_obj.nd_auto_increment_start_value, table_obj.nd_auto_increment_end_value, read_limit):
                    nd_logger.debug(f"[DbScanner] Creating QC task at nd_auto_increment_start_value {start_value} for table {table_obj.metadata.table_name}")
                    task = Task.create_task(
                        chain=chain,
                        fn=qc_task,
                        arguments={
                            "table_id": table_id,
                            "nd_auto_increment_start_value": start_value,
                            "nd_auto_increment_end_value": start_value + read_limit,
                            "important_cols": important_cols,
                            "source_connection_string": src_conn_str,
                            "dest_connection_string": dest_conn_str,
                            "qc_config": qc_config,
                        },
                        dependencies=[],
                    )
                qc_tasks_list.append(task)
                task = Task.create_task(
                    chain=chain,
                    fn=qc_cleanup_task,
                    arguments={
                        "table_id": table_id,
                        "source_connection_string": src_conn_str,
                        "dest_connection_string": dest_conn_str,
                    },
                    dependencies=qc_tasks_list,
                )
                nd_logger.info(f"[DbScanner] QC tasks and cleanup task created for table_id {table_id}")
            table_obj.qc.qc_status = Status.IN_PROGRESS
            table_obj.qc.save()
            nd_logger.info(f"[DbScanner] Set QC status to IN_PROGRESS for table_id {table_id}")
        except Exception as e:
            nd_logger.error(f"[DbScanner] scan_table exception: {e}\n{traceback.format_exc()}")
            pass
        return {"success": True}


def qc_task(
    table_id: int,
    nd_auto_increment_start_value: int,
    nd_auto_increment_end_value: int,
    important_cols: list[str],
    source_connection_string: str,
    dest_connection_string: str,
    qc_config: dict,
    dependencies: list[int] = [],
):
    nd_logger.info(f"[qc_task] QC task started for table_id={table_id}, read_offset={nd_auto_increment_start_value}, limit={nd_auto_increment_end_value}")
    table_obj = TableModel.objects.get(id=table_id)
    try:
        data_generator = DataGenerator(
            NDDBHandler(source_connection_string), NDDBHandler(dest_connection_string)
        )
        read_limit, source_df, dest_df = data_generator.generate_sample(
            table_obj.metadata.table_name, nd_auto_increment_start_value, nd_auto_increment_end_value, important_cols
        )
        nd_logger.info(f"[qc_task] Loaded sample data: source_df({source_df.shape}), dest_df({dest_df.shape}) for table_id={table_id}")
        source_df = source_df.sort_values(by="nd_auto_increment_id").reset_index(drop=True)
        dest_df = dest_df.sort_values(by="nd_auto_increment_id").reset_index(drop=True)

        detectors, patient_mapping_df, encounter_mapping_df = DbScanner.get_structured_detectors(
            source_df, table_obj, qc_config
        )
        columns_qc_result = {}
        for col_name, detector in detectors:
            nd_logger.debug(f"[qc_task] Running is_deidentified for column {col_name}")
            result = detector.is_deidentified(
                before_df=source_df,
                after_df=dest_df
            )
            columns_qc_result[col_name] = result

        pii_data = LoadPIIData.load(patient_mapping_df, encounter_mapping_df, table_obj)
        notes_columns = [
            col_conf["column_name"]
            for col_conf in table_obj.metadata.table_details_for_ui["columns_details"]
            if col_conf["de_identification_rule"] in ["NOTES", "GENERIC_NOTES"]
        ]
        pii_config = get_pii_config()
        detector = UnstructuredDetector(columns_names=notes_columns, pii_config=pii_config, default_offset=settings.DEFAULT_OFFSET_VALUE)
        patient_identifiers = get_patient_identifiers(table_obj.metadata.table_details_for_ui)
        appointment_to_nd_pid_mapping_df = pd.DataFrame()
        qc_result = detector.is_deidentified(
            source_df,
            dest_df,
            patient_id_cols=patient_identifiers["patient_id"],
            encounter_id_cols=patient_identifiers["encounter_id"],
            appointment_id_cols=patient_identifiers["appointment_id"],
            patient_mapping_df=patient_mapping_df,
            pii_data_df=pii_data['pii_df'],
            global_data_df=pii_data['global_df'],
            secondary_pii_dfs=pii_data['secondary_dfs'],
            enc_to_nd_pid_mapping_df=encounter_mapping_df,
            appointment_to_nd_pid_mapping_df=appointment_to_nd_pid_mapping_df
        )
        final_output = {**columns_qc_result, **qc_result}
        nd_logger.info(f"[qc_task] Finished QC task for table_id={table_id}, returning result keys: {list(final_output.keys())}")
        return final_output
    except Exception as e:
        error_message = f"failure reason: {e}\n, traceback: {traceback.format_exc()}"
        nd_logger.error(f"[qc_task] QC task FAILED for table_id={table_id}: {error_message}")
        table_obj.qc.qc_status = Status.FAILED
        table_obj.qc.qc_result = {"error_reason": "code failure", "error_message": error_message}
        table_obj.qc.save()
        prepare_message = {
            "alert_type": f"QC Failed for the table: {table_obj.metadata.table_name}, due to code failure",
            "dump_identifier": f"{table_obj.incremental_queue.id} - {table_obj.incremental_queue.queue_name}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e

def get_types(d):
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):  # recursive check for nested dicts
            result[k] = get_types(v)
        else:
            result[k] = type(v).__name__
    return result


def qc_cleanup_task(
    table_id: int,
    source_connection_string: str,
    dest_connection_string: str,
    dependencies: list[int] = [],
):
    nd_logger.info(f"[qc_cleanup_task] Starting cleanup for table_id={table_id}")
    try:
        dest_handler = NDDBHandler(dest_connection_string)
        table_obj = TableModel.objects.get(id=table_id)
        columns_qc_result = dependencies.copy()
        ignore_row_count = IgnoreRowsDeIdentificaiton.objects.filter(
            queue_id=table_obj.incremental_queue.id, table_name=table_obj.metadata.table_name
        ).count()
        dest_count = dest_handler.get_rows_count(table_obj.metadata.table_name)
        souce_count = table_obj.nd_auto_increment_end_value - table_obj.nd_auto_increment_start_value + 1
        data_count_result = {
            "source_rows_count": souce_count,
            "dest_rows_count": int(dest_count) if dest_count is not None else 0,
            "ignore_rows_count": int(ignore_row_count) if ignore_row_count is not None else 0,
        }
        nd_logger.info(f"[qc_cleanup_task] Data count result: {data_count_result}")

        final_qc_result = get_final_result_after_merge(data_count_result, columns_qc_result)
        nd_logger.info(f"[qc_cleanup_task] Final QC result: {final_qc_result.get('final_qc_result', {})}")
        output = OutputSchemaForTable(**data_count_result, **final_qc_result)
        
        # Convert numpy/pandas int64 types to native Python int for JSON serialization
        output = convert_numpy_types_to_python(output)
        table_obj.qc.qc_result = output
        table_obj.qc.qc_status = (
            Status.COMPLETED if final_qc_result['final_qc_result']["is_qc_passed"] else Status.FAILED
        )
        table_obj.qc.save()
        nd_logger.info(f"[qc_cleanup_task] QC status for table_id {table_id}: {table_obj.qc.qc_status}")
        if table_obj.qc.qc_status == Status.FAILED:
            prepare_message = {
                "alert_type": f"QC Failed for the table: {table_obj.metadata.table_name}, Due to Improper deidentification",
                "dump_identifier": f"{table_obj.incremental_queue.id} - {table_obj.incremental_queue.queue_name}",
                "traceback": str(traceback.format_exc())
            }
            alert_sender.send_message(prepare_message)
            nd_logger.warning(f"[qc_cleanup_task] Sent alert: QC failed for table {table_obj.metadata.table_name}")
        send_qc_websockte_event(table_obj)
        nd_logger.info(f"[qc_cleanup_task] Sent websocket update for table_id {table_id}")
        return {"success": True}
    except Exception as e:
        error_message = f"failure reason: {e}\n, traceback: {traceback.format_exc()}"
        nd_logger.error(f"[qc_cleanup_task] Cleanup FAILURE for table_id={table_id}: {error_message}")
        table_obj.qc.qc_status = Status.FAILED
        error_result = {"error_reason": "code failure", "error_message": str(error_message)}
        # Convert any numpy types to ensure JSON serialization works
        error_result = convert_numpy_types_to_python(error_result)
        table_obj.qc.qc_result = error_result
        table_obj.qc.save()
        prepare_message = {
            "alert_type": f"QC Failed for the table: {table_obj.metadata.table_name}, due to code failure",
            "dump_identifier": f"{table_obj.incremental_queue.id} - {table_obj.incremental_queue.queue_name}",
            "traceback": traceback.format_exc()
        }
        send_qc_websockte_event(table_obj)
        alert_sender.send_message(prepare_message)
        raise e


def get_final_result_after_merge(
    data_count_result: dict, columns_qc_results: list[dict[str:ColumnQCResult]]
):
    nd_logger.info(f"[get_final_result_after_merge] Merging QC results.")
    final_qc_result = FinalQCResult(is_qc_passed=True, reason="", failure_nd_auto_incr_ids=[])

    merge_columns_qc_result: dict = {
    }
    if data_count_result["source_rows_count"] != (
        data_count_result["dest_rows_count"] + data_count_result["ignore_rows_count"]
    ):
        nd_logger.warning(f"[get_final_result_after_merge] Data discrepancy present: source_rows_count={data_count_result['source_rows_count']}, dest_rows_count={data_count_result['dest_rows_count']}, ignore_rows_count={data_count_result['ignore_rows_count']}")
        final_qc_result["is_qc_passed"] = False
        final_qc_result["reason"] += f"data discrepancy present. "

    for column_qc_result in columns_qc_results:
        for colname, result in column_qc_result.items():
            # Ensure we're working with Python native types
            passed_count = int(result["passed_count"]) if result.get("passed_count") is not None else 0
            failed_count = int(result["failed_count"]) if result.get("failed_count") is not None else 0
            existing_passed = merge_columns_qc_result.get(colname, {}).get("passed_count", 0)
            existing_failed = merge_columns_qc_result.get(colname, {}).get("failed_count", 0)
            
            merge_columns_qc_result[colname] = {
                "passed_count": passed_count + existing_passed,
                "failed_count": failed_count + existing_failed,
                "failure_reasons": list(
                    set(result.get("failure_reasons", []))
                ),
            }
            merge_columns_qc_result[colname]['remarks'] = result.get("remarks", {})
            
            # Convert failure_nd_auto_incr_ids to Python native ints
            failure_ids = [int(x) for x in result.get("failure_nd_auto_incr_ids", [])]
            final_qc_result["failure_nd_auto_incr_ids"] = list(
                set(
                    failure_ids
                    + final_qc_result["failure_nd_auto_incr_ids"]
                )
            )

    columns_failed = []
    for colname, result in merge_columns_qc_result.items():
        merge_columns_qc_result[colname]["failure_reasons"] = ". ".join(
            [
                QCErrors[reason]
                for reason in merge_columns_qc_result[colname]["failure_reasons"]
            ]
        )
        if result["failed_count"] > 0:
            final_qc_result["is_qc_passed"] = False
            columns_failed.append(colname)
    if len(columns_failed) > 0:
        final_qc_result["reason"] += "QC Failed on columns: " + ", ".join(
            columns_failed
        )
        nd_logger.warning(f"[get_final_result_after_merge] QC failed on columns: {columns_failed}")
    # Sample the failed nd_auto_incr_ids for the final result
    final_qc_result["failure_nd_auto_incr_ids"] = random.sample(final_qc_result["failure_nd_auto_incr_ids"], min(25, len(final_qc_result["failure_nd_auto_incr_ids"])))
    nd_logger.info(f"[get_final_result_after_merge] Final QC result: {final_qc_result}")
    return {
        "final_qc_result": final_qc_result,
        "column_qc_result": merge_columns_qc_result,
    }

def send_qc_websockte_event(table_obj: TableModel):
    """
    Send QC status update via websocket for real-time UI updates
    """
    nd_logger.debug(f"[send_qc_websockte_event] Attempting to send websocket event for table_id={table_obj.id}, table_name={table_obj.metadata.table_name}")
    if not settings.WEBSOCKET_EVENT_ENABLED:
        nd_logger.info(f"[send_qc_websockte_event] WebSocket event not enabled, skipping for table_id={table_obj.id}")
        return
    
    # Import here to avoid circular imports
    from ndwebsocket.utils import broadcast_table_status_update
    
    # Map status values to UI-friendly statuses
    status_mapping = {
        Status.COMPLETED: 'completed',
        Status.FAILED: 'failed', 
        Status.IN_PROGRESS: 'in_progress',
        Status.NOT_STARTED: 'not_started'
    }
    
    ui_status = status_mapping.get(table_obj.qc.qc_status, 'not_started')
    
    # Send structured table status update
    broadcast_table_status_update(
        table_id=table_obj.id,
        table_name=table_obj.metadata.table_name,
        process_type='qc',
        status=ui_status,
        message=f"QC {ui_status} for table {table_obj.metadata.table_name}",
        error_details=table_obj.qc.qc_result if table_obj.qc.qc_status == Status.FAILED else None,
        save_to_db=False  # Don't save to DB for UI updates
    )
    nd_logger.info(f"[send_qc_websockte_event] WebSocket event sent for table_id={table_obj.id} status={ui_status}")
