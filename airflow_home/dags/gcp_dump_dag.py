"""
GCP dump DAG: dump DEIDENTIFIED_SCHEMA tables to local SQL files and upload to GCS.
Tables come from optional gcp_transfer.csv (TABLE_NAME column) or all tables in schema.
Two tasks: run_dump (mysqldump) then run_upload (gsutil to GCS).
"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from services.gcp_dump_service import (
    get_tables_to_dump,
    run_mysqldump_dump,
    upload_dump_to_gcs,
)


with DAG(
    dag_id="gcp_dump_upload",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "dump", "upload"],
) as dag_gcp_dump:

    @task
    def run_dump() -> dict:
        """Get tables (CSV or all in DEIDENTIFIED_SCHEMA), run mysqldump to local SQL files."""
        tables = get_tables_to_dump()
        result = run_mysqldump_dump(tables=tables)
        if result["failed"]:
            failed_list = ", ".join(f["table"] for f in result["failed"])
            first_err = result["failed"][0].get("error", "")
            raise RuntimeError(
                f"Dump failed for {len(result['failed'])} table(s): {failed_list}. First error: {first_err}"
            )
        return {
            "output_dir": result["output_dir"],
            "dumped": result["dumped"],
            "stats_path": result.get("stats_path"),
        }

    @task
    def run_upload(dump_result: dict) -> dict:
        """Upload dump output_dir to GCS (gsutil)."""
        if dump_result.get("dumped", 0) == 0:
            return {"bucket": None, "destination_prefix": None, "uploaded": 0, "errors": []}
        return upload_dump_to_gcs(source_folder=dump_result["output_dir"])

    run_upload(run_dump())
