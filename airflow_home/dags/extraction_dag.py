from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine, text

from services.config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE,
)
from services.extraction_service import extract_table
from services.nd_date_service import add_extraction_date_to_all_tables
from services.merge_service import merge_incremental_to_historical
from services.diff_schema_service import copy_historical_to_diff_schema
from services.schema_reset_service import reset_incremental_schema as reset_schema
from services.deid_runner import run_deid_pipeline_for_airflow, create_deid_tasks_for_queue, wait_for_deid_completion
from services.mapping_master_service import update_mapping_and_master_tables
from services.worker_lifecycle import clear_worker_logs, start_workers, stop_workers

SCHEMA = "ATHENAONE"

# Deid workers: number to start and optional conda env (set to None to use current Python)
DEID_WORKERS = 2
DEID_CONDA_ENV = "airflow_inc"  # or None

# --- Tuning knobs ---
BATCH_SIZE = 20        # Number of tables per task (800 tables / 20 = 40 tasks in UI)
MAX_ACTIVE_TASKS = 5   # Max batches running in parallel (so max 5 x 20 = 100 concurrent Snowflake queries)

# --- Testing: hardcode table names here when testing specific tables ---
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
"VITALSIGN"
    # "OTHER_TABLE",
]


with DAG(
    dag_id="Athenaone_Master_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=["athenaone", "incremental"],
) as dag:

    @task
    def reset_incremental_schema() -> dict:
        """
        Drop and recreate the incremental schema (database) so each run starts clean.
        Runs first, before any extraction.
        """
        return reset_schema()

    @task
    def get_table_batches() -> list[list[str]]:
        """
        Discover all views in Snowflake schema and split into batches.
        Returns a list of batches, e.g.:
          [["TABLE_1", "TABLE_2", ...], ["TABLE_21", ...], ...]

        Each batch becomes one mapped task instance in the UI,
        so 800 tables / 20 per batch = 40 tasks instead of 800.
        """
        engine = create_engine(
            f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
            f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SCHEMA}"
            f"?warehouse={SNOWFLAKE_WAREHOUSE}",
            connect_args={"insecure_mode": True},
            pool_pre_ping=True,
        )

        query = text("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = :schema
            ORDER BY TABLE_NAME
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"schema": SCHEMA})
            all_tables = [row[0] for row in result.fetchall()]

        if not all_tables:
            raise ValueError(f"No views found in schema {SCHEMA}. Check schema name and permissions.")

        # Split into batches of BATCH_SIZE
        batches = [
            all_tables[i:i + BATCH_SIZE]
            for i in range(0, len(all_tables), BATCH_SIZE)
        ]

        return batches

    @task
    def get_test_batches() -> list[list[str]]:
        """
        Returns a single batch containing TEST_TABLE_NAMES.
        Use this instead of get_table_batches() when testing.
        """
        if not TEST_TABLE_NAMES:
            raise ValueError("TEST_TABLE_NAMES is empty. Add table names to test.")
        return [TEST_TABLE_NAMES]

    @task
    def extract_batch(batch: list[str]) -> dict:
        """
        Extract a batch of tables sequentially within a single task.
        Tables in a batch run one after another (no parallel Snowflake hits within a batch).
        Batches themselves run in parallel up to MAX_ACTIVE_TASKS.

        Returns a summary dict for easy monitoring in XCom.
        """
        results = {
            "total": len(batch),
            "success": [],
            "failed": [],
            "no_data": [],
        }

        for table_name in batch:
            try:
                result = extract_table(table_name)
                if result.get("rows_inserted", 0) == 0:
                    results["no_data"].append(table_name)
                else:
                    results["success"].append({
                        "table": table_name,
                        "rows_inserted": result["rows_inserted"],
                    })
            except Exception as e:
                # Don't let one failed table kill the whole batch
                results["failed"].append({
                    "table": table_name,
                    "error": str(e),
                })

        # Fail the task if any table in the batch failed,
        # so it shows red in UI and you can investigate
        if results["failed"]:
            failed_names = [f["table"] for f in results["failed"]]
            raise RuntimeError(
                f"Batch had {len(results['failed'])} failures: {failed_names}. "
                f"Succeeded: {len(results['success'])}, No data: {len(results['no_data'])}. "
                f"Details: {results['failed']}"
            )

        return results

    @task
    def add_nd_extracted_date() -> dict:
        """
        Add nd_extracted_date column to all tables in incremental schema (if missing)
        and set it to current date. Runs once after all extract_batch tasks complete.
        """
        return add_extraction_date_to_all_tables()

    @task
    def merge_to_historical() -> dict:
        """
        Merge data from incremental schema to historical schema.
        Creates missing tables in historical, then INSERT INTO ... SELECT for common columns.
        Runs once after add_nd_extracted_date.
        """
        return merge_incremental_to_historical()

    @task
    def get_tables_for_diff(batches: list[list[str]]) -> list[str]:
        """
        Flatten batch list to a single list of table names (tables extracted in this run).
        Passed to copy_to_diff_schema so only these tables are copied to diff.
        """
        return [t for batch in batches for t in batch]

    @task
    def copy_to_diff_schema(tables_to_copy: list[str]) -> dict:
        """
        Create schema diff_<current_date> if not exists and copy from historical
        only the tables extracted in this run (tables_to_copy), inserting only rows
        where nd_extracted_date = CURDATE(). Runs after merge_to_historical.
        """
        return copy_historical_to_diff_schema(tables_to_copy=tables_to_copy)

    @task
    def run_deid_pipeline(diff_result: dict) -> dict:
        """
        Write override file, run nd_auto_increment_id, register_dump. Does not run mapping/master
        or create deid tasks (separate tasks). Expects diff_result from copy_to_diff_schema with key diff_schema.
        """
        if not diff_result or "diff_schema" not in diff_result:
            raise ValueError("copy_to_diff_schema did not return diff_schema")
        return run_deid_pipeline_for_airflow(diff_result["diff_schema"])

    @task
    def update_mapping_master(pipe_result: dict) -> dict:
        """
        Update mapping and master tables for the queue. Separate task so validation can be added after this.
        Expects pipe_result with key queue_id (from run_deid_pipeline).
        """
        if not pipe_result or "queue_id" not in pipe_result:
            raise ValueError("run_deid_pipeline did not return queue_id")
        return update_mapping_and_master_tables(pipe_result["queue_id"])

    @task
    def create_deid_tasks(pipe_result: dict) -> dict:
        """
        Create deid tasks for all tables in the queue. Runs after mapping/master (and any validation).
        Expects pipe_result with queue_id (from update_mapping_master).
        """
        if not pipe_result or "queue_id" not in pipe_result:
            raise ValueError("update_mapping_master did not return queue_id")
        return create_deid_tasks_for_queue(pipe_result["queue_id"])

    @task
    def wait_for_deid(deid_result: dict) -> dict:
        """
        Poll until all worker tasks for this queue are done. Assumes spine_worker runs elsewhere.
        Expects deid_result from create_deid_tasks with key queue_id.
        """
        if not deid_result or "queue_id" not in deid_result:
            raise ValueError("create_deid_tasks did not return queue_id")
        return wait_for_deid_completion(deid_result["queue_id"])

    @task
    def start_deid_workers(mapping_result: dict) -> dict:
        """
        Clear worker logs and start deid workers so they can pick up tasks.
        Runs after mapping_master, before create_deid_tasks. Passes through mapping_result (queue_id) for downstream.
        """
        out = clear_worker_logs()
        out_start = start_workers(n=DEID_WORKERS, conda_env=DEID_CONDA_ENV)
        return {**(mapping_result or {}), "cleared_logs": out.get("cleared", []), **out_start}

    @task
    def stop_deid_workers(_wait_result: dict) -> dict:
        """
        Stop deid workers after all tasks are done.
        Runs after wait_for_deid.
        """
        return stop_workers()

    # get_table_batches() returns a list of lists
    # expand() creates one task per batch → 40 tasks in UI instead of 800
    # TEST: only tables in TEST_TABLE_NAMES (paste table names above)
    batches = get_test_batches()
    # FULL RUN: all views from Snowflake
    # batches = get_table_batches()
    tables_for_diff = get_tables_for_diff(batches)
    reset_task = reset_incremental_schema()
    expanded = extract_batch.expand(batch=batches)
    add_date_task = add_nd_extracted_date()
    merge_task = merge_to_historical()
    diff_task = copy_to_diff_schema(tables_to_copy=tables_for_diff)
    deid_run_task = run_deid_pipeline(diff_task)
    mapping_master_task = update_mapping_master(deid_run_task)
    start_workers_task = start_deid_workers(mapping_master_task)
    create_deid_tasks_task = create_deid_tasks(start_workers_task)
    wait_deid_task = wait_for_deid(create_deid_tasks_task)
    stop_workers_task = stop_deid_workers(wait_deid_task)
    reset_task >> batches >> expanded >> add_date_task >> merge_task >> diff_task >> deid_run_task >> mapping_master_task >> start_workers_task >> create_deid_tasks_task >> wait_deid_task >> stop_workers_task
    batches >> tables_for_diff >> diff_task