"""
GCP dump DAG: dump DEIDENTIFIED_SCHEMA tables to local SQL files and upload to GCS.
Tables come from optional gcp_transfer.csv (TABLE_NAME column) or all tables in schema.
"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from services.gcp_dump_service import run_gcp_dump_pipeline


with DAG(
    dag_id="gcp_dump_upload",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "dump", "upload"],
) as dag_gcp_dump:

    @task
    def run_dump_and_upload() -> dict:
        """Get tables (CSV or all in DEIDENTIFIED_SCHEMA), mysqldump, upload to GCS."""
        return run_gcp_dump_pipeline()

    run_dump_and_upload()
