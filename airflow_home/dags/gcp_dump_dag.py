"""
GCP dump DAG: dump DEIDENTIFIED_SCHEMA tables under gcp_dump/<MMDDYYYY>/, upload to
gs://bucket/EHR/<MMDDYYYY>/ (see config GCP_DESTINATION_PREFIX).
Tasks: clear_dump_dir → run_dump (mysqldump) → run_upload (gsutil to GCS).

Params (settable at trigger time):
  schema: MySQL schema to dump from. Defaults to DEIDENTIFIED_SCHEMA from config.
  tables: Comma-separated table names. Defaults to gcp_transfer.csv (or all tables if CSV missing).
"""
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from services.config import DEIDENTIFIED_SCHEMA, GCP_DUMP_DAG_PREFIX
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
    params={
        "schema": Param(
            default=DEIDENTIFIED_SCHEMA,
            type="string",
            description="MySQL schema to dump from. Defaults to DEIDENTIFIED_SCHEMA in config.",
        ),
        "tables": Param(
            default="",
            type="string",
            description="Comma-separated table names to dump. Defaults to gcp_transfer.csv (or all tables in schema if CSV missing).",
        ),
    },
) as dag_gcp_dump:

    @task
    def clear_dump_dir() -> dict:
        """Clear today's date folder under gcp_dump (e.g. gcp_dump/03182026/) before dump."""
        ctx = get_current_context()
        schema = ctx["params"].get("schema") or DEIDENTIFIED_SCHEMA
        return clear_dump_directory(schema=schema)

    @task
    def run_dump() -> dict:
        """Get tables (param list, or gcp_transfer.csv, or all in schema), run mysqldump."""
        import logging
        ctx = get_current_context()
        schema = ctx["params"].get("schema") or DEIDENTIFIED_SCHEMA
        tables_param = ctx["params"].get("tables", "")
        if tables_param and tables_param.strip():
            requested = [t.strip() for t in tables_param.split(",") if t.strip()]
            # filter to only tables that exist in the schema — skip missing ones
            existing = set(get_tables_to_dump(schema=schema))
            existing_upper = {t.upper(): t for t in existing}
            tables = []
            for t in requested:
                if t.upper() in existing_upper:
                    tables.append(existing_upper[t.upper()])
                else:
                    logging.warning("[gcp_dump] Table %r not found in schema %r — skipping.", t, schema)
        else:
            # falls back to gcp_transfer.csv, then all tables if CSV missing
            tables = get_tables_to_dump(schema=schema)
        result = run_mysqldump_dump(schema=schema, tables=tables)
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
            destination_prefix=f"{GCP_DUMP_DAG_PREFIX.strip('/')}/{date_folder}",
        )

    dump_result = run_dump()
    clear_dump_dir() >> dump_result >> run_upload(dump_result)
