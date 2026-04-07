"""
DAG: gcp_restore_and_append

Runs on a schedule and watches every machine's GCS prefix for new dump folders.
A folder is considered "ready" when it contains sql_dump_stats.csv (written last
by gcp_dump_service after upload completes). After a successful restore+append
the DAG writes a _processed marker to GCS so the folder is never re-processed.

One DAG run processes ALL machines that have pending data:
  1. scan_gcs            — find oldest unprocessed date_folder per machine; skip if none
  2. run_qc_all          — verify GCS transfer checksums for each machine
  3. restore_staging_all — drop+create each machine's STAGING_SCHEMA, restore .sql files
  4. append_incremental_all — copy staging → INCREMENTAL_SCHEMA (INSERT IGNORE) per machine
  5. clear_staging_all   — wipe each machine's STAGING_SCHEMA
  6. mark_processed_all  — write _processed marker to each machine's GCS folder

Machine configs (bucket, watch_prefix, staging/incremental schemas) come from
GCP_related/config.py → MACHINE_RESTORE_CONFIGS.

Manual trigger conf examples:
  {"machine_name": "machine1", "date_folder": "04062026"}  → force one machine+folder
  {}                                                         → scan all machines normally
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

# ── Make services importable ──────────────────────────────────────────────────
_DAG_DIR     = os.path.dirname(__file__)                                        # GCP_related/airflow_home/dags/
_SERVICES    = os.path.abspath(os.path.join(_DAG_DIR, "..", "services"))       # GCP_related/airflow_home/services/
_GCP_RELATED = os.path.abspath(os.path.join(_DAG_DIR, "..", ".."))             # GCP_related/
for _p in (_SERVICES, _GCP_RELATED):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ─────────────────────────────────────────────────────────────────────────────

from config import GCS_POLL_INTERVAL_MINUTES, MACHINE_RESTORE_CONFIGS

with DAG(
    dag_id="gcp_restore_and_append",
    start_date=datetime(2026, 1, 1),
    schedule=f"*/{GCS_POLL_INTERVAL_MINUTES} * * * *",
    catchup=False,
    max_active_runs=1,          # never overlap — one restore run at a time
    tags=["gcp", "restore", "append", "incremental"],
) as dag:

    @task
    def scan_gcs(**context) -> list[dict]:
        """
        Scan all machines for the oldest unprocessed dump folder each.
        Returns a list of {name, gcs_bucket, gcs_watch_prefix,
                            staging_schema, incremental_schema,
                            date_folder, folder_prefix}.
        Manual trigger with {machine_name, date_folder} in conf forces one machine.
        Raises AirflowSkipException when nothing is pending.
        """
        from gcp_restore_append_service import (
            scan_all_machines_for_unprocessed,
            _gcs_folder_prefix,
        )

        dag_run = context.get("dag_run")
        conf = dict((dag_run.conf or {}) if dag_run else {})

        # Manual trigger: process one specific machine + date_folder
        if conf.get("machine_name") and conf.get("date_folder"):
            machine_name = conf["machine_name"]
            date_folder  = conf["date_folder"]
            mc = next((m for m in MACHINE_RESTORE_CONFIGS if m["name"] == machine_name), None)
            if mc is None:
                raise ValueError(f"Unknown machine_name in conf: {machine_name!r}")
            return [{**mc, "date_folder": date_folder, "folder_prefix": _gcs_folder_prefix(mc, date_folder)}]

        pending = scan_all_machines_for_unprocessed()
        if not pending:
            raise AirflowSkipException("No new dump folders found across all machines — nothing to process.")
        print(f"[scan_gcs] Found {len(pending)} machine(s) with pending data: "
              + ", ".join(f"{p['name']}:{p['date_folder']}" for p in pending))
        return pending

    @task
    def run_qc_all(scan_results: list[dict]) -> list[dict]:
        """Verify GCS transfer checksums for every pending machine folder."""
        from gcp_restore_append_service import run_qc as _run_qc
        results = []
        for item in scan_results:
            result = _run_qc(item["date_folder"], item)
            results.append(result)
        return results

    @task
    def restore_staging_all(scan_results: list[dict]) -> list[dict]:
        """Drop+create each machine's staging schema and restore .sql files from GCS."""
        from gcp_restore_append_service import restore_to_staging
        results = []
        for item in scan_results:
            result = restore_to_staging(item["date_folder"], item)
            results.append(result)
        return results

    @task
    def append_incremental_all(restore_results: list[dict], scan_results: list[dict]) -> list[dict]:
        """Copy every table from each machine's staging schema into its incremental schema."""
        from gcp_restore_append_service import append_staging_to_incremental
        results = []
        for item in scan_results:
            result = append_staging_to_incremental(item)
            if result["failed"]:
                raise RuntimeError(
                    f"[{item['name']}] Append failed for {result['failed']} table(s): "
                    + str(result["failed_tables"])
                )
            results.append(result)
        return results

    @task
    def clear_staging_all(_append_results: list[dict], scan_results: list[dict]) -> list[dict]:
        """Wipe each machine's staging schema after a successful append."""
        from gcp_restore_append_service import clear_staging_schema
        results = []
        for item in scan_results:
            result = clear_staging_schema(item)
            results.append(result)
        return results

    @task
    def mark_processed_all(scan_results: list[dict], _clear_results: list[dict]) -> list[dict]:
        """Write _processed marker to each machine's GCS folder."""
        from gcp_restore_append_service import mark_folder_processed
        done = []
        for item in scan_results:
            mark_folder_processed(item["date_folder"], item)
            done.append({"machine": item["name"], "date_folder": item["date_folder"], "status": "marked_processed"})
        return done

    scan_out     = scan_gcs()
    qc_out       = run_qc_all(scan_out)
    restore_out  = restore_staging_all(scan_out)
    append_out   = append_incremental_all(restore_out, scan_out)
    clear_out    = clear_staging_all(append_out, scan_out)
    done         = mark_processed_all(scan_out, clear_out)

    scan_out >> qc_out >> restore_out >> append_out >> clear_out >> done
