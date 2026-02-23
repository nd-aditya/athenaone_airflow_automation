from airflow import DAG
from airflow.sdk import task
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
from services.extraction_date_service import add_extraction_date_to_all_tables
from services.merge_service import merge_incremental_to_historical

SCHEMA = "ATHENAONE"

# --- Tuning knobs ---
BATCH_SIZE = 20        # Number of tables per task (800 tables / 20 = 40 tasks in UI)
MAX_ACTIVE_TASKS = 5   # Max batches running in parallel (so max 5 x 20 = 100 concurrent Snowflake queries)

# --- Testing: hardcode table names here when testing specific tables ---
TEST_TABLE_NAMES = [
    "MEDICATION"
    # "OTHER_TABLE",
]


with DAG(
    dag_id="snowflake_incremental_extraction",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=["snowflake", "incremental"],
) as dag:


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

    # get_table_batches() returns a list of lists
    # expand() creates one task per batch → 40 tasks in UI instead of 800
    # TEST: only tables in TEST_TABLE_NAMES (paste table names above)
    batches = get_test_batches()
    # FULL RUN: all views from Snowflake
    # batches = get_table_batches()
    expanded = extract_batch.expand(batch=batches)
    add_date_task = add_nd_extracted_date()
    merge_task = merge_to_historical()
    expanded >> add_date_task >> merge_task