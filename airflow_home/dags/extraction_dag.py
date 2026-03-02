from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, inspect, text

from services.config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    HISTORICAL_SCHEMA,
)
from services.extraction_service import extract_table
from services.nd_date_service import add_extraction_date_to_all_tables
from services.merge_service import (
    merge_incremental_to_historical,
    ensure_historical_indexes_and_update_flags,
)
from services.diff_schema_service import copy_historical_to_diff_schema, update_diff_schema_history_and_drop_old
from services.deid_merge_service import merge_deid_to_merged
from services.schema_reset_service import reset_incremental_schema as reset_schema
from services.deid_runner import (
    run_deid_pipeline_for_airflow,
    create_deid_tasks_for_queue,
    wait_for_deid_completion,
    get_table_ids_for_queue_by_names,
)
from services.mapping_master_service import update_mapping_and_master_tables
from services.worker_lifecycle import clear_worker_logs, start_workers, stop_workers

# Snowflake schemas to extract from. Each can have table_rename_map for MySQL target names
# (e.g. ATHENAONE.appointment -> appointment_2 so it does not clash with scheduling.appointment).
EXTRACT_SOURCE_CONFIGS = [
    {"schema": "ATHENAONE", "table_rename_map": {"APPOINTMENT": "appointment_2"}},
    {"schema": "scheduling", "table_rename_map": {}},
    {"schema": "financials", "table_rename_map": {}},
]

DEID_WORKERS = 10
DEID_CONDA_ENV = "py39"

BATCH_SIZE = 20
MAX_ACTIVE_TASKS = 5
DEID_TABLE_BATCH_SIZE = 50

# MySQL table names in historical/diff (ATHENAONE.appointment -> appointment_2 to avoid clash with scheduling.appointment)
TEST_TABLE_NAMES = [
    "ALLERGY",
    "APPOINTMENT",
    "APPOINTMENTELIGIBILITYINFO",
    "APPOINTMENTNOTE",
    "APPOINTMENTVIEW",
    "CHART",
    "CHARTQUESTIONNAIRE",
    "CHARTQUESTIONNAIREANSWER",
    "CLINICALENCOUNTER",
    "CLINICALENCOUNTERDATA",
    "CLINICALENCOUNTERDIAGNOSIS",
    "CLINICALENCOUNTERDXICD10",
    "CLINICALENCOUNTERPREPNOTE",
    "CLINICALORDERTYPE",
    "clinicalprescription",
    "CLINICALRESULT",
    "CLINICALRESULTOBSERVATION",
    "CLINICALSERVICE",
    "CLINICALSERVICEPROCEDURECODE",
    "CLINICALTEMPLATE",
    "document",
    "FDB_RMIID1",
    "FDB_RNDC14",
    "ICDCODEALL",
    "INSURANCEPACKAGE",
    "medication",
    "PATIENT",
    "PATIENTALLERGY",
    "PATIENTALLERGYREACTION",
    "PATIENTFAMILYHISTORY",
    "PATIENTINSURANCE",
    "patientmedication",
    "PATIENTPASTMEDICALHISTORY",
    "PATIENTSOCIALHISTORY",
    "PATIENTSURGERY",
    "PATIENTSURGICALHISTORY",
    "PROCEDURECODE",
    "PROCEDURECODEREFERENCE",
    "SNOMED",
    "SOCIALHXFORMRESPONSE",
    "SOCIALHXFORMRESPONSEANSWER",
    "SURGICALHISTORYPROCEDURE",
    "visit",
    "VITALATTRIBUTEREADING",
    "VITALSIGN",
]

# =============================================================================
# DAG 1: Extract + Merge only (all tables, table batches). No diff/deid.
# =============================================================================
with DAG(
    dag_id="Athenaone_Extract_Merge",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=["athenaone", "extract", "merge"],
) as dag_extract_merge:

    @task
    def reset_incremental_schema() -> dict:
        return reset_schema()

    @task
    def get_table_batches() -> list[list[dict]]:
        """Return batches of {schema, table_name, target_table_name} for all configured schemas."""
        from services.config import SNOWFLAKE_DATABASE

        all_items = []
        for config in EXTRACT_SOURCE_CONFIGS:
            schema_name = config["schema"]
            rename_map = config.get("table_rename_map") or {}
            engine = create_engine(
                f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
                f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{schema_name}"
                f"?warehouse={SNOWFLAKE_WAREHOUSE}",
                connect_args={"insecure_mode": True},
                pool_pre_ping=True,
            )
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = :schema ORDER BY TABLE_NAME"),
                    {"schema": schema_name},
                )
                tables = [row[0] for row in result.fetchall()]
            for table_name in tables:
                target_table_name = rename_map.get(table_name, table_name)
                all_items.append({
                    "schema": schema_name,
                    "table_name": table_name,
                    "target_table_name": target_table_name,
                })
        if not all_items:
            raise ValueError("No views found in any configured schema.")
        return [all_items[i : i + BATCH_SIZE] for i in range(0, len(all_items), BATCH_SIZE)]

    @task
    def extract_batch(batch: list[dict]) -> dict:
        results = {"total": len(batch), "success": [], "failed": [], "no_data": []}
        for item in batch:
            schema_name = item["schema"]
            table_name = item["table_name"]
            target_table_name = item["target_table_name"]
            try:
                result = extract_table(table_name, schema=schema_name, target_table_name=target_table_name)
                if result.get("rows_inserted", 0) == 0:
                    results["no_data"].append(target_table_name)
                else:
                    results["success"].append({"table": target_table_name, "rows_inserted": result["rows_inserted"]})
            except Exception as e:
                results["failed"].append({"table": target_table_name, "error": str(e)})
        if results["failed"]:
            msg = "; ".join(
                f"{f['table']}: {f.get('error', '')}" for f in results["failed"]
            )
            raise RuntimeError(f"Batch failures: {msg}")
        return results

    @task
    def add_nd_extracted_date() -> dict:
        return add_extraction_date_to_all_tables()

    @task
    def merge_to_historical() -> dict:
        return merge_incremental_to_historical()

    @task
    def ensure_indexes_and_update_flags() -> dict:
        """Ensure idx_*_pk and idx_*_norm exist on historical tables, then set nd_active_flag."""
        return ensure_historical_indexes_and_update_flags()

    batches = get_table_batches()
    reset_task = reset_incremental_schema()
    expanded = extract_batch.expand(batch=batches)
    add_date_task = add_nd_extracted_date()
    merge_task = merge_to_historical()
    ensure_indexes_task = ensure_indexes_and_update_flags()
    reset_task >> batches >> expanded >> add_date_task >> merge_task >> ensure_indexes_task


# =============================================================================
# DAG 2: Copy to diff → deid for priority tables only (TEST_TABLE_NAMES list).
# =============================================================================
with DAG(
    dag_id="Athenaone_Deid_Priority_Tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=["athenaone", "deid", "priority_tables"],
) as dag_deid_priority:

    @task
    def copy_to_diff_priority() -> dict:
        return copy_historical_to_diff_schema(tables_to_copy=TEST_TABLE_NAMES)

    @task
    def run_deid_pipeline(diff_result: dict) -> dict:
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("copy_to_diff_schema did not return diff_schema")
        return run_deid_pipeline_for_airflow(diff_result["diff_schema"])

    @task
    def update_mapping_master(pipe_result: dict) -> dict:
        if not pipe_result or "queue_id" not in pipe_result:
            raise ValueError("run_deid_pipeline did not return queue_id")
        return update_mapping_and_master_tables(pipe_result["queue_id"])

    @task
    def get_priority_table_ids(mapping_result: dict) -> dict:
        """Return dict with queue_id and table_ids for priority tables (single payload for downstream)."""
        if not mapping_result or "queue_id" not in mapping_result:
            raise ValueError("mapping_result missing queue_id")
        table_ids = get_table_ids_for_queue_by_names(mapping_result["queue_id"], TEST_TABLE_NAMES)
        return {**(mapping_result or {}), "table_ids": table_ids}

    @task
    def start_deid_workers(mapping_result: dict) -> dict:
        out = clear_worker_logs()
        out_start = start_workers(n=DEID_WORKERS, conda_env=DEID_CONDA_ENV)
        return {**(mapping_result or {}), "cleared_logs": out.get("cleared", []), **out_start}

    @task
    def create_deid_tasks(pipe_result: dict) -> dict:
        if not pipe_result or "queue_id" not in pipe_result or "table_ids" not in pipe_result:
            raise ValueError("pipe_result missing queue_id or table_ids")
        out = create_deid_tasks_for_queue(pipe_result["queue_id"], table_ids=pipe_result["table_ids"])
        return {**out, "table_ids": pipe_result["table_ids"]}

    @task
    def wait_for_deid(deid_result: dict) -> dict:
        if not deid_result or "queue_id" not in deid_result or "table_ids" not in deid_result:
            raise ValueError("deid_result missing queue_id or table_ids")
        return wait_for_deid_completion(deid_result["queue_id"], table_ids=deid_result["table_ids"])

    @task
    def stop_deid_workers(_: dict) -> dict:
        return stop_workers()

    @task
    def merge_deid_to_merged_task(diff_result: dict) -> dict:
        """Merge diff_<date>_deid into DEIDENTIFIED_SCHEMA (config)."""
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("diff_result missing diff_schema")
        deid_schema = diff_result["diff_schema"] + "_deid"
        return merge_deid_to_merged(deid_schema)

    @task
    def trim_diff_schemas(diff_result: dict) -> dict:
        """Update diff schema history file and drop diff/deid schemas older than last 3 runs."""
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("diff_result missing diff_schema")
        return update_diff_schema_history_and_drop_old(diff_result["diff_schema"], keep_last_n=3)

    diff_task = copy_to_diff_priority()
    deid_run_task = run_deid_pipeline(diff_task)
    mapping_master_task = update_mapping_master(deid_run_task)
    priority_table_ids_task = get_priority_table_ids(mapping_master_task)
    start_workers_task = start_deid_workers(priority_table_ids_task)
    create_deid_tasks_task = create_deid_tasks(start_workers_task)
    wait_deid_task = wait_for_deid(create_deid_tasks_task)
    stop_workers_task = stop_deid_workers(wait_deid_task)
    merge_deid_task = merge_deid_to_merged_task(diff_task)
    trim_task = trim_diff_schemas(diff_task)
    diff_task >> deid_run_task >> mapping_master_task >> priority_table_ids_task >> start_workers_task >> create_deid_tasks_task >> wait_deid_task >> stop_workers_task >> merge_deid_task >> trim_task


# =============================================================================
# DAG 3: Copy to diff → deid for remaining tables (linear, same pattern as DAG 2).
# =============================================================================
with DAG(
    dag_id="Athenaone_Deid_Remaining_Tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=["athenaone", "deid", "remaining_tables"],
) as dag_deid_remaining:

    @task
    def get_remaining_tables() -> list[str]:
        """Tables in historical schema that are not in TEST_TABLE_NAMES."""
        connection_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
        engine = create_engine(connection_str, pool_pre_ping=True)
        inspector = inspect(engine)
        hist_tables = set(inspector.get_table_names(schema=HISTORICAL_SCHEMA))
        test_set = set(t.upper() for t in TEST_TABLE_NAMES) | set(TEST_TABLE_NAMES)
        remaining = [t for t in hist_tables if t not in test_set]
        engine.dispose()
        return remaining

    @task
    def copy_to_diff_remaining(tables_to_copy: list[str]) -> dict:
        return copy_historical_to_diff_schema(tables_to_copy=tables_to_copy)

    @task
    def run_deid_pipeline_rem(diff_result: dict) -> dict:
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("copy_to_diff_schema did not return diff_schema")
        return run_deid_pipeline_for_airflow(diff_result["diff_schema"])

    @task
    def update_mapping_master_rem(pipe_result: dict) -> dict:
        if not pipe_result or "queue_id" not in pipe_result:
            raise ValueError("run_deid_pipeline did not return queue_id")
        return update_mapping_and_master_tables(pipe_result["queue_id"])

    @task
    def get_remaining_table_ids(mapping_result: dict, diff_result: dict) -> dict:
        """Return dict with queue_id and table_ids for all remaining tables (single payload for downstream)."""
        if not mapping_result or "queue_id" not in mapping_result:
            raise ValueError("mapping_result missing queue_id")
        tables_to_copy = diff_result.get("tables_to_copy") or []
        if not tables_to_copy:
            return {**(mapping_result or {}), "table_ids": []}
        table_ids = get_table_ids_for_queue_by_names(mapping_result["queue_id"], tables_to_copy)
        return {**(mapping_result or {}), "table_ids": table_ids}

    @task
    def start_deid_workers_rem(payload: dict) -> dict:
        out = clear_worker_logs()
        out_start = start_workers(n=DEID_WORKERS, conda_env=DEID_CONDA_ENV)
        return {**(payload or {}), "cleared_logs": out.get("cleared", []), **out_start}

    @task
    def create_deid_tasks_rem(pipe_result: dict) -> dict:
        if not pipe_result or "queue_id" not in pipe_result or "table_ids" not in pipe_result:
            raise ValueError("pipe_result missing queue_id or table_ids")
        if not pipe_result["table_ids"]:
            return {**pipe_result, "deid_tasks_created_for_tables": 0}
        out = create_deid_tasks_for_queue(pipe_result["queue_id"], table_ids=pipe_result["table_ids"])
        return {**out, "table_ids": pipe_result["table_ids"]}

    @task
    def wait_for_deid_rem(deid_result: dict) -> dict:
        if not deid_result or "queue_id" not in deid_result or "table_ids" not in deid_result:
            raise ValueError("deid_result missing queue_id or table_ids")
        if not deid_result["table_ids"]:
            return deid_result
        return wait_for_deid_completion(deid_result["queue_id"], table_ids=deid_result["table_ids"])

    @task
    def stop_deid_workers_rem(_: dict) -> dict:
        return stop_workers()

    @task
    def merge_deid_to_merged_task_rem(diff_result: dict) -> dict:
        """Merge diff_<date>_deid into DEIDENTIFIED_SCHEMA (config)."""
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("diff_result missing diff_schema")
        deid_schema = diff_result["diff_schema"] + "_deid"
        return merge_deid_to_merged(deid_schema)

    @task
    def trim_diff_schemas_rem(diff_result: dict) -> dict:
        """Update diff schema history file and drop diff/deid schemas older than last 3 runs."""
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("diff_result missing diff_schema")
        return update_diff_schema_history_and_drop_old(diff_result["diff_schema"], keep_last_n=3)

    remaining_tables_task = get_remaining_tables()
    diff_task = copy_to_diff_remaining(remaining_tables_task)
    deid_run_task = run_deid_pipeline_rem(diff_task)
    mapping_master_task = update_mapping_master_rem(deid_run_task)
    remaining_table_ids_task = get_remaining_table_ids(mapping_master_task, diff_task)
    start_workers_task = start_deid_workers_rem(remaining_table_ids_task)
    create_deid_tasks_task = create_deid_tasks_rem(start_workers_task)
    wait_deid_task = wait_for_deid_rem(create_deid_tasks_task)
    stop_workers_task = stop_deid_workers_rem(wait_deid_task)
    merge_deid_task = merge_deid_to_merged_task_rem(diff_task)
    trim_task = trim_diff_schemas_rem(diff_task)
    (remaining_tables_task >> diff_task >> deid_run_task >> mapping_master_task
     >> remaining_table_ids_task >> start_workers_task >> create_deid_tasks_task
     >> wait_deid_task >> stop_workers_task >> merge_deid_task >> trim_task)
