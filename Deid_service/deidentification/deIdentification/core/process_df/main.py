from typing import Union, Optional, List
import traceback
from nd_api_v2.models import Table as TableModel, IncrementalQueue
from nd_api_v2.models.configs import get_mapping_table_config
from nd_api.schemas.table_config import TableDetailsForUI, ColumnDetailsForUI
from deIdentification.nd_logger import nd_logger
from core.dbPkg import NDDBHandler
from core.ops_df.jointables import ReferenceMappingDataFrameJoiner
from core.ops_df.utility import DistinctValueFetcher, join_dataframes
import time
import pandas as pd
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    MetaData,
    create_engine,
    or_,
    func,
    select,
    cast,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from core.process_df.base import DeIdentifier, Rules
from django.conf import settings
from core.process_df.columns_type_detector import ColumnsTypeDetector
from core.process_df.rowhandler import InvalidRowHandler
from typing import Union
from portal.alerts import alert_sender
from core.dbPkg.mapping_loader import ENCOUNTER_MAPPING_TABLE, PATIENT_MAPPING_TABLE, ENCOUNTER_MAPPING_TABLE_ENCID_COL, PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL
from nd_api_v2.models.scheduler_config import SchedulerConfig

Base = declarative_base()


def get_key_phi_column_list(column_details: ColumnDetailsForUI) -> tuple:
    encounter_id_columns = []
    patient_id_columns = {}
    appointment_id_columns = []

    if column_details is None:
        raise ValueError("Table Config Not set")

    for column in column_details:
        rule = column.get("de_identification_rule")
        column_name = column.get("column_name")
        print(rule, column_name)

        if column.get("is_phi") and rule:
            if rule.startswith("PATIENT_"):
                patient_id_columns.setdefault(rule, []).append(column_name)
            elif rule == Rules.ENCOUNTER_ID.value:
                encounter_id_columns.append(column_name)
            elif rule == Rules.APPOINTMENT_ID.value:
                appointment_id_columns.append(column_name)
    return (encounter_id_columns, patient_id_columns, appointment_id_columns)


class PatientIdentifierResolver:
    def __init__(self, key_phi_columns: dict, possible_patient_identifier_columns: list[str]):
        self.key_phi_columns = key_phi_columns
        self.possible_patient_identifier_columns = possible_patient_identifier_columns

        nd_logger.info(f"[{self.__class__.__name__}] Initialized with key_phi_columns: {key_phi_columns}")
        nd_logger.info(f"[{self.__class__.__name__}] Possible identifier columns: {possible_patient_identifier_columns}")

        self.encounter_group = self._create_fixed_group("encounter_mapping")
        self.appointment_group = self._create_fixed_group("appointment_mapping")
        self.identifier_groups = self._create_dynamic_identifier_groups()

    def _create_fixed_group(self, suffix: str) -> dict:
        group = {
            "nd_patient_id": f"nd_patient_id_from_{suffix}",
            "offset": f"offset_from_{suffix}",
        }
        for col in self.possible_patient_identifier_columns:
            group[col] = f"{col}_from_{suffix}"
        nd_logger.debug(f"[{self.__class__.__name__}] Created fixed group for '{suffix}': {group}")
        return group

    def _create_dynamic_identifier_groups(self) -> list[dict]:
        groups = []
        for rule_name, cols in self.key_phi_columns[1].items():
            key_col = cols[0] if cols else None
            
            if not key_col:
                nd_logger.warning(f"[{self.__class__.__name__}] No column found for rule '{rule_name}'")
                continue
            
            identifer_column = rule_name.split("_")[-1].lower()
            group = {
                "nd_patient_id": f"nd_patient_id_from_{identifer_column}_mapping",
                "offset": f"offset_from_{identifer_column}_mapping",
            }
            print("key_col", key_col)
            print("identifer_col", identifer_column)
            for col in self.possible_patient_identifier_columns:
                if identifer_column == col:
                    group[col] = key_col
                else:
                    group[col] = f"{col}_from_{identifer_column}_mapping"

            groups.append(group)
            nd_logger.debug(f"[{self.__class__.__name__}] Created dynamic identifier group for '{identifer_column}': {group}")
        return groups

    def _resolve_column(self, df: pd.DataFrame, candidates: list[str]) -> pd.Series:
        existing = [col for col in candidates if col in df.columns]
        nd_logger.debug(f"[{self.__class__.__name__}] Resolving columns from: {candidates}")
        if not existing:
            nd_logger.warning(f"[{self.__class__.__name__}] No candidate columns found. Returning all None.")
            return pd.Series([None] * len(df), index=df.index)
        nd_logger.info(f"[{self.__class__.__name__}] Using columns: {existing}")
        return df[existing].bfill(axis=1).infer_objects(copy=False).iloc[:, 0]

    def _resolve_offset_column(self, df: pd.DataFrame, candidates: list[str]) -> pd.Series:
        existing = [col for col in candidates if col in df.columns]
        nd_logger.debug(f"[{self.__class__.__name__}] Resolving offset from: {candidates}")
        if not existing:
            nd_logger.info(f"[{self.__class__.__name__}] No offset columns found. Using default offset.")
            return pd.Series([settings.DEFAULT_OFFSET_VALUE] * len(df), index=df.index)
        nd_logger.info(f"[{self.__class__.__name__}] Using offset columns: {existing}")
        return df[existing].bfill(axis=1).infer_objects(copy=False).iloc[:, 0]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        nd_logger.info(f"[{self.__class__.__name__}] Starting patient identifier resolution...")

        # Priority: encounter_group > identifier_groups (in order) > appointment_group
        all_groups = [self.encounter_group] + self.identifier_groups + [self.appointment_group]

        # Resolve offset
        offset_candidates = [group["offset"] for group in all_groups if "offset" in group]
        df["_resolved_offset"] = self._resolve_offset_column(df, offset_candidates)

        # Resolve nd_patient_id
        nd_patient_id_candidates = [group["nd_patient_id"] for group in all_groups if "nd_patient_id" in group]
        df["_resolved_nd_patient_id"] = self._resolve_column(df, nd_patient_id_candidates)

        # Resolve each patient identifier field (chartid, pid, etc.)
        for col in self.possible_patient_identifier_columns:
            candidates = [group.get(col) for group in all_groups if group.get(col)]
            df[f"_resolved_{col}"] = self._resolve_column(df, candidates)


        # Optional: drop intermediate columns (debug log before drop)
        all_used_columns = set()
        for group in all_groups:
            all_used_columns.update(group.values())

        #Flatten all original key columns to preserve
        original_key_cols = set(
            col
            for rule_cols in self.key_phi_columns[1].values()
            for col in rule_cols
        )

        #Drop only those that are not original keys and are still in DataFrame
        to_drop = [col for col in all_used_columns if col in df.columns and col not in original_key_cols]

        if to_drop:
            nd_logger.debug(f"[{self.__class__.__name__}] Dropping intermediate columns (excluding original keys): {to_drop}")
            df.drop(columns=to_drop, inplace=True)

        nd_logger.info(f"[{self.__class__.__name__}] Resolution completed. Final DataFrame shape: {df.shape}")
        return df



class JoinMapping:
    def __init__(self, df, key_phi_columns, table_obj: TableModel):
        self.df = df
        self.key_phi_columns = key_phi_columns
        self.mapping_db_config = get_mapping_table_config()
        nd_logger.info(
            f"[{self.__class__.__name__}] Initialized with table: {table_obj.metadata.table_name}"
        )
        self._get_mapping_table_connection()

    def _get_mapping_table_connection(self):
        nd_logger.info(f"[{self.__class__.__name__}] Connecting to mapping DB...")
        connection_string = self.mapping_db_config["connection_str"]
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        nd_logger.info(
            f"[{self.__class__.__name__}] DB connection and session established."
        )

    def close_connection(self):
        self.session.close()
        self.engine.dispose()
        nd_logger.info(f"[{self.__class__.__name__}] Connection closed.")


    def get_possible_patient_identifier_columns(self) -> List[str]:
        """
        Returns all patient identifier columns from the patient_mapping_table,
        excluding 'created_at', 'updated_at', and 'nd_patient_id'.
        """
        metadata = MetaData()
        patient_mapping = Table("patient_mapping_table", metadata, autoload_with=self.engine)
        
        excluded_columns = {"offset", "created_at", "updated_at", "nd_patient_id", "registration_date"}
        possible_patient_identifier_columns = [
            col.name for col in patient_mapping.columns
            if col.name not in excluded_columns
        ]
        
        nd_logger.info(f"[{self.__class__.__name__}] Possible Patient identifier columns: {possible_patient_identifier_columns}")
        return possible_patient_identifier_columns


    def _get_distinct_ids(self, index: int, label: str) -> list:
        nd_logger.info(f"[{self.__class__.__name__}] Getting distinct {label}...")
        if index >= len(self.key_phi_columns) or not self.key_phi_columns[index]:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No {label} column configured."
            )
            return []

        fetcher = DistinctValueFetcher(self.df)
        return fetcher.get_distinct_values(self.key_phi_columns[index][0])

    def _get_distinct_encounterids(self):
        return self._get_distinct_ids(0, "encounter IDs")

    def _get_distinct_appointmentids(self):
        return self._get_distinct_ids(3, "appointment IDs")

    def apply_patient_mappings(self, possible_patient_identifier_columns: list) -> pd.DataFrame:
        """
        For each patient-related rule:
        1. Gets distinct values from the corresponding column in the data.
        2. Queries patient_mapping_table for those values using the appropriate key.
        3. Joins the result back into self.df on the specified key.
        
        :param identifier_columns: List of columns to include from the mapping table (in addition to filter key).
        :return: Updated DataFrame with joined mapping values.
        """
        if len(self.key_phi_columns) < 2 or not self.key_phi_columns[1]:
            nd_logger.warning(f"[{self.__class__.__name__}] No patient ID rules found.")
            return self.df

        df = self.df.copy()
        metadata = MetaData()
        patient_mapping_table = Table("patient_mapping_table", metadata, autoload_with=self.engine)

        for rule, columns in self.key_phi_columns[1].items():
            if not columns:
                continue

            left_col = columns[0]
            right_col = rule.split("_")[-1].lower()
                                                         
            nd_logger.info(f"[{self.__class__.__name__}] Processing rule '{rule}' using column '{left_col}' on '{right_col}'")

            # Step 1: Get distinct values from left_col in self.df
            distinct_values = df[left_col].dropna().unique().tolist()
            if not distinct_values:
                nd_logger.warning(f"[{self.__class__.__name__}] No values found for column '{left_col}'")
                continue

            # Step 2: Build and execute query on patient_mapping_table
            columns_to_select = [patient_mapping_table.c[right_col]] + [
                patient_mapping_table.c[col] for col in possible_patient_identifier_columns if col != right_col] + [patient_mapping_table.c["offset"], patient_mapping_table.c["nd_patient_id"]]

            stmt = select(*columns_to_select).where(patient_mapping_table.c[right_col].in_(distinct_values))

            with self.engine.connect() as conn:
                df_patient_mapping = pd.read_sql(stmt, conn)

            nd_logger.info(f"[{self.__class__.__name__}] Retrieved {len(df_patient_mapping)} rows from patient_mapping_table for rule '{rule}'")

            # Step 3: Join mapping back into main DataFrame
            if not df_patient_mapping.empty:
                df = join_dataframes(df, df_patient_mapping, left_on=left_col, right_on=right_col,
                    how="left", right_suffix=f"from_{right_col}_mapping", drop_right_join_column=True,)
            else:
                nd_logger.warning(f"[{self.__class__.__name__}] No mappings found for values in column '{left_col}'")

        return df
    

    def _get_patient_mapping_from_nd_patient_id(self, nd_patient_ids: List, possible_patient_identifier_columns: List[str]) -> pd.DataFrame:
        if not nd_patient_ids:
            nd_logger.warning(f"[{self.__class__.__name__}] No patient IDs provided.")
            return pd.DataFrame(columns=possible_patient_identifier_columns+["nd_patient_id", "offset"])

        nd_logger.info(f"[{self.__class__.__name__}] Fetching patient mappings for {len(nd_patient_ids)} IDs...")

        metadata = MetaData()
        table = Table("patient_mapping_table", metadata, autoload_with=self.engine)

        columns_to_select = [
            table.c.nd_patient_id,
            table.c.offset
        ] + [table.c[col] for col in possible_patient_identifier_columns]

        stmt = select(*columns_to_select).where(table.c.nd_patient_id.in_(nd_patient_ids))

        with self.engine.connect() as conn:
            df = pd.read_sql(stmt, conn)

        nd_logger.info(f"[{self.__class__.__name__}] Retrieved {len(df)} rows from patient_mapping_table.")
        return df


    def _get_mapping_with_patient_join(
        self,
        ids: List,
        table_name: str,
        id_column: str,
        nd_id_column: str,
        right_suffix: str,
        possible_patient_identifier_columns: List[str]
    ) -> Union[pd.DataFrame, None]:
        if not ids:
            nd_logger.warning(f"[{self.__class__.__name__}] No IDs provided for {table_name}.")
            return None

        nd_logger.info(f"[{self.__class__.__name__}] Fetching mappings from {table_name} for {len(ids)} IDs...")

        metadata = MetaData()
        mapping_table = Table(table_name, metadata, autoload_with=self.engine)

        stmt = select(
            mapping_table.c[id_column],
            cast(mapping_table.c[nd_id_column], String(50)).label(nd_id_column),
            mapping_table.c.nd_patient_id.label("nd_patient_id"),
        ).where(mapping_table.c[id_column].in_(ids))

        with self.engine.connect() as conn:
            df_mapping = pd.read_sql(stmt, conn, dtype={nd_id_column: "object"})

        nd_logger.info(f"[{self.__class__.__name__}] Retrieved {len(df_mapping)} rows from {table_name}.")

        fetcher = DistinctValueFetcher(df_mapping)
        nd_patient_ids = fetcher.get_distinct_values("nd_patient_id")
        nd_logger.info(f"[{self.__class__.__name__}] Extracted {len(nd_patient_ids)} unique patient IDs from {table_name}.")

        df_patient_mapping = self._get_patient_mapping_from_nd_patient_id(nd_patient_ids, possible_patient_identifier_columns)
        df_joined = join_dataframes(df_mapping, df_patient_mapping,
            left_on="nd_patient_id", right_on="nd_patient_id", how="left", right_suffix=right_suffix, drop_left_join_column=True, )

        nd_logger.info(f"[{self.__class__.__name__}] Joined {table_name} and patient mapping. Final rows: {len(df_joined)}")

        return df_joined

    
    def _get_encounter_mapping(self, encounter_ids: List[str], possible_patient_identifier_columns: List[str]) -> Union[pd.DataFrame, None]:
        return self._get_mapping_with_patient_join(
            ids=encounter_ids,
            table_name=ENCOUNTER_MAPPING_TABLE,
            id_column=ENCOUNTER_MAPPING_TABLE_ENCID_COL,
            nd_id_column=ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL,
            right_suffix="from_encounter_mapping",
            possible_patient_identifier_columns=possible_patient_identifier_columns,
        )

    def _get_appointment_mapping(self, appointment_ids: List[str], possible_patient_identifier_columns: List[str]) -> Union[pd.DataFrame, None]:
        return self._get_mapping_with_patient_join(
            ids=appointment_ids,
            table_name="appointment_mapping_table",
            id_column="appointment_id",
            nd_id_column="nd_appointment_id",
            right_suffix="from_appointment_mapping",
            possible_patient_identifier_columns=possible_patient_identifier_columns,
        )



def _get_columns_schema_mapping(table_config: TableDetailsForUI):
    schema_mapping = {}
    rule_to_schema_mapping = ColumnsTypeDetector.get_columns_definations(table_config)

    for col_conf in table_config["columns_details"]:
        if not col_conf.get("is_phi"):
            continue

        rule_name = col_conf["de_identification_rule"]
        column_name = col_conf["column_name"]

        try:
            rule_enum = Rules[rule_name]
            schema_mapping[column_name] = rule_to_schema_mapping[rule_enum]
        except KeyError:
            # Handle dynamic rules like PATIENT_PID, PATIENT_CHARTID, etc.
            if rule_name.startswith("PATIENT_"):
                # Map them to the PATIENT_ID base schema
                schema_mapping[column_name] = rule_to_schema_mapping[Rules.PATIENT_ID]
            else:
                raise ValueError(f"Unknown rule in column config: {rule_name}")
    # schema_mapping['nd_deidentification_datetime'] = {"type": DateTime, "null": False, "default": func.now()}
    return schema_mapping



def start_de_identification_for_table_with_df(
    table_id: int, batch_size: int, offset: int, table_config: TableDetailsForUI, dependencies: list = []
):
    table_obj = TableModel.objects.get(id=table_id)
    if table_obj is None:
        raise Exception(f"Table with id {table_id} not found")
    try:
        pd.set_option("display.float_format", "{:.0f}".format)
        incremental_queue_obj: IncrementalQueue = table_obj.incremental_queue
        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")
        nd_logger.info(f"getting connection for source db: {table_obj.metadata.table_name}")
        source_db_connection: NDDBHandler = NDDBHandler(scheduler_config.get_source_connection_str())

        read_start = time.time()
        nd_logger.info(f"Reading the rows from table: {table_obj.metadata.table_name}")
        df = source_db_connection.get_table_as_dataframe(table_obj.metadata.table_name, batch_size, offset)

        nd_logger.info(f"start getting the reference columns for table_id {table_id} with batch size of {batch_size} and offset of {offset}")
        key_phi_columns = get_key_phi_column_list(table_config["columns_details"])
        print(key_phi_columns)

        reference_mapping_obj = ReferenceMappingDataFrameJoiner(
            source_db_connection, df, table_obj.id, table_config, key_phi_columns)
        df, key_phi_columns = reference_mapping_obj.join_dataframe()

        mapping_obj = JoinMapping(df, key_phi_columns, table_obj)
        possible_patient_identifier_columns = mapping_obj.get_possible_patient_identifier_columns()
        df = mapping_obj.apply_patient_mappings(possible_patient_identifier_columns)


        distinct_encounterIds = mapping_obj._get_distinct_encounterids()
        df_encounter_mapping = mapping_obj._get_encounter_mapping(distinct_encounterIds, possible_patient_identifier_columns)
        if df_encounter_mapping is not None:
            df = join_dataframes(df, df_encounter_mapping, left_on=key_phi_columns[0][0], right_on="encounter_id",
                how="left", right_suffix="from_encounter_mapping", drop_right_join_column=True,)
        
        distinct_appointmentIds = mapping_obj._get_distinct_appointmentids()
        df_appointment_mapping = mapping_obj._get_appointment_mapping(distinct_appointmentIds, possible_patient_identifier_columns)
        
        if df_appointment_mapping is not None:
            df = join_dataframes( df, df_appointment_mapping, left_on=key_phi_columns[2][0],
                right_on="appointment_id", how="left", drop_right_join_column=True,)
            

        mapping_obj.close_connection()

        resolver = PatientIdentifierResolver(key_phi_columns, possible_patient_identifier_columns)
        df = resolver.transform(df)
        nd_logger.info(f"DataFrame columns: {df.columns.tolist()}")

        row_handler = InvalidRowHandler(queue_id=incremental_queue_obj.id, table_name=table_obj.metadata.table_name, key_phi_columns=key_phi_columns)
        df = row_handler.handle(df)

        """
        ignore rows where patient_ids or nd_patient_ids are not equal and remove those rows and log them into ignore rows.
        ignore rows where no nd_patient_id is found (in either primary encounter_id, ) - DONE
        sort offset column - DONE
        """

       

        deidentifier = DeIdentifier(
            df=df,
            config=table_config["columns_details"],
            table_obj=table_obj.metadata,
            incremental_queue_obj=incremental_queue_obj,
            key_phi_columns=key_phi_columns,
            possible_patient_identifier_columns=possible_patient_identifier_columns
        )

        df = deidentifier.apply_rules()
        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")

        destination_db: NDDBHandler = NDDBHandler(scheduler_config.get_deid_connection_str())
        column_schema_mapping = _get_columns_schema_mapping(table_config)
        source_db_connection.create_table_in_dest_if_not_exists(
            table_obj.metadata.table_name, destination_db, column_type_mapping=column_schema_mapping
        )
        destination_db.insert_dataframe_in_batches(df, table_name=table_obj.metadata.table_name)

        source_db_connection.close()
        destination_db.close()
        return {"table_id": table_id, "batch_size": batch_size, "offset": offset}
    except Exception as e:
        prepare_message = {
            "alert_type": "Master Table Generation task Failed",
            "table_identifier": f"{table_id} - {table_obj.metadata.table_name}",
            "incremental_queue_identifier": f"{incremental_queue_obj.id} - {incremental_queue_obj.queue_name}",
            "queue_identifier": f"{incremental_queue_obj.id} - {incremental_queue_obj.queue_name}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e
