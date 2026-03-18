"""
GCP dump DAG: dump DEIDENTIFIED_SCHEMA tables under gcp_dump/<MMDDYYYY>/, upload to
gs://bucket/EHR/<MMDDYYYY>/ (see config GCP_DESTINATION_PREFIX).
Tasks: clear_dump_dir → run_dump (mysqldump) → run_upload (gsutil to GCS).
"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from services.gcp_dump_service import (
    clear_dump_directory,
    gcp_dump_date_root,
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
    def clear_dump_dir() -> dict:
        """Clear today's date folder under gcp_dump (e.g. gcp_dump/03182026/) before dump."""
        return clear_dump_directory()

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
            "gcs_prefix": result.get("gcs_prefix"),
            "date_folder": result.get("date_folder"),
        }

    @task
    def run_upload(dump_result: dict) -> dict:
        """Upload date folder to GCS under EHR/<MMDDYYYY>/ (gsutil)."""
        if dump_result.get("dumped", 0) == 0:
            return {"bucket": None, "destination_prefix": None, "uploaded": 0, "errors": []}
        date_folder = dump_result.get("date_folder")
        if not date_folder:
            return {
                "bucket": None,
                "destination_prefix": None,
                "uploaded": 0,
                "errors": [{"error": "date_folder missing from dump_result"}],
            }
        return upload_dump_to_gcs(
            source_folder=gcp_dump_date_root(date_folder),
            destination_prefix=dump_result.get("gcs_prefix"),
        )

    dump_result = run_dump()
    clear_dump_dir() >> dump_result >> run_upload(dump_result)
