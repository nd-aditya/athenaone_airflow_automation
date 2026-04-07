"""
Multi-machine Restore → Append → Clear pipeline.

Each function that touches a specific machine accepts a `machine_config` dict
from MACHINE_RESTORE_CONFIGS:
  {name, gcs_bucket, gcs_watch_prefix, staging_schema, incremental_schema}

Public API:
  scan_all_machines_for_unprocessed()       → list of pending machine items
  scan_for_unprocessed_dump(machine_config) → oldest pending folder for one machine, or None
  mark_folder_processed(date_folder, mc)
  run_qc(date_folder, mc)
  restore_to_staging(date_folder, mc)
  append_staging_to_incremental(mc)
  clear_staging_schema(mc)
  refresh_incremental_schema(mc)
"""
from __future__ import annotations

import osa
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, inspect, text

# ── Paths ─────────────────────────────────────────────────────────────────────
_SERVICES_DIR     = os.path.dirname(__file__)                                  # GCP_related/airflow_home/services/
_GCP_RELATED_ROOT = os.path.abspath(os.path.join(_SERVICES_DIR, "..", ".."))   # GCP_related/

for _p in (_SERVICES_DIR, _GCP_RELATED_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from config import (
    MACHINE_RESTORE_CONFIGS,
    GCS_DUMP_STATS_FILE,
    GCS_PROCESSED_MARKER,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    MYSQL_PORT,
    RESTORE_MAX_WORKERS,
    APPEND_MAX_WORKERS,
    TRUNCATE_MAX_WORKERS,
)
from gcp_transfer_restore import run_restore, run_transfer_qc


# ── Engine ────────────────────────────────────────────────────────────────────

def _engine():
    """Root-level engine (no default schema) — accesses any schema via FQN."""
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/",
        pool_pre_ping=True,
        pool_size=max(APPEND_MAX_WORKERS, TRUNCATE_MAX_WORKERS) + 2,
        max_overflow=2,
    )


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _gcs_folder_prefix(machine_config: dict, date_folder: str) -> str:
    """Build the GCS folder prefix: gcs_watch_prefix/date_folder/"""
    return f"{machine_config['gcs_watch_prefix'].strip('/')}/{date_folder}/"


def scan_for_unprocessed_dump(machine_config: dict) -> dict | None:
    """
    Scan one machine's GCS bucket/prefix for sub-folders that contain
    sql_dump_stats.csv but NOT a _processed marker.
    Returns the earliest unprocessed {date_folder, folder_prefix}, or None.
    """
    from google.cloud import storage as gcs
    client = gcs.Client()
    watch_prefix = machine_config["gcs_watch_prefix"].strip("/") + "/"
    blobs = client.list_blobs(machine_config["gcs_bucket"], prefix=watch_prefix)

    folders_with_stats: set[str] = set()
    folders_processed:  set[str] = set()

    for blob in blobs:
        # blob.name: nd_incremental/04062026/schema/TABLE.sql
        parts = blob.name.split("/")
        if len(parts) < 2:
            continue
        date_folder = parts[1]
        filename    = parts[-1]
        if filename == GCS_DUMP_STATS_FILE:
            folders_with_stats.add(date_folder)
        if filename == GCS_PROCESSED_MARKER:
            folders_processed.add(date_folder)

    unprocessed = sorted(folders_with_stats - folders_processed)
    if not unprocessed:
        return None
    date_folder = unprocessed[0]
    return {"date_folder": date_folder, "folder_prefix": _gcs_folder_prefix(machine_config, date_folder)}


def scan_all_machines_for_unprocessed() -> list[dict]:
    """
    Scan every machine in MACHINE_RESTORE_CONFIGS for pending (unprocessed) dump folders.
    Returns a list of items, each merging the machine_config fields with
    {date_folder, folder_prefix}.  Empty list → nothing to do.
    """
    pending = []
    for mc in MACHINE_RESTORE_CONFIGS:
        result = scan_for_unprocessed_dump(mc)
        if result:
            pending.append({**mc, **result})
    return pending


def mark_folder_processed(date_folder: str, machine_config: dict) -> None:
    """Write a _processed marker to GCS so this date_folder is not re-processed."""
    from google.cloud import storage as gcs
    client = gcs.Client()
    bucket = client.bucket(machine_config["gcs_bucket"])
    marker_path = f"{_gcs_folder_prefix(machine_config, date_folder)}{GCS_PROCESSED_MARKER}"
    bucket.blob(marker_path).upload_from_string("", content_type="text/plain")
    print(f"[GCS:{machine_config['name']}] Marked as processed: {marker_path}")


# ── Public API ────────────────────────────────────────────────────────────────

def run_qc(date_folder: str, machine_config: dict) -> dict:
    """Run GCS transfer QC for the given machine + date folder."""
    folder_prefix = _gcs_folder_prefix(machine_config, date_folder)
    run_transfer_qc(bucket_name=machine_config["gcs_bucket"], folder_name=folder_prefix)
    return {
        "machine": machine_config["name"],
        "status": "passed",
        "bucket": machine_config["gcs_bucket"],
        "folder_prefix": folder_prefix,
    }


def restore_to_staging(date_folder: str, machine_config: dict) -> dict:
    """Drop + recreate staging_schema, then restore .sql files from GCS into it."""
    staging = machine_config["staging_schema"]
    engine = _engine()
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS `{staging}`"))
        conn.execute(text(f"CREATE DATABASE `{staging}`"))
        conn.commit()
    engine.dispose()

    run_restore(
        bucket_name=machine_config["gcs_bucket"],
        folder_prefix=_gcs_folder_prefix(machine_config, date_folder),
        db_name=staging,
        max_workers=RESTORE_MAX_WORKERS,
    )
    return {"machine": machine_config["name"], "staging_schema": staging, "date_folder": date_folder, "status": "restored"}


def _append_one_table(table: str, staging: str, incremental: str, engine) -> dict:
    stats = {"table": table, "inserted": 0, "error": None}
    try:
        with engine.connect() as conn:
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS `{incremental}`.`{table}` "
                f"LIKE `{staging}`.`{table}`"
            ))
            result = conn.execute(text(
                f"INSERT IGNORE INTO `{incremental}`.`{table}` "
                f"SELECT * FROM `{staging}`.`{table}`"
            ))
            conn.commit()
        stats["inserted"] = result.rowcount
    except Exception as e:
        stats["error"] = str(e)
    return stats


def append_staging_to_incremental(machine_config: dict) -> dict:
    """
    Copy every table from staging_schema into incremental_schema.
    Creates incremental_schema and missing tables automatically.
    Uses INSERT IGNORE to skip duplicate rows.
    """
    staging     = machine_config["staging_schema"]
    incremental = machine_config["incremental_schema"]
    engine = _engine()

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{incremental}`"))
        conn.commit()

    tables  = inspect(engine).get_table_names(schema=staging)
    results = []

    with ThreadPoolExecutor(max_workers=APPEND_MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(_append_one_table, t, staging, incremental, engine): t
            for t in tables
        }
        for future in as_completed(future_to_table):
            results.append(future.result())

    engine.dispose()

    failed = [r for r in results if r["error"]]
    return {
        "machine": machine_config["name"],
        "incremental_schema": incremental,
        "total_tables": len(results),
        "total_inserted": sum(r["inserted"] for r in results),
        "succeeded": len(results) - len(failed),
        "failed": len(failed),
        "failed_tables": [{"table": r["table"], "error": r["error"]} for r in failed],
    }


def clear_staging_schema(machine_config: dict) -> dict:
    """Drop + recreate staging_schema (wipes it for the next restore run)."""
    staging = machine_config["staging_schema"]
    engine  = _engine()
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS `{staging}`"))
        conn.execute(text(f"CREATE DATABASE `{staging}`"))
        conn.commit()
    engine.dispose()
    return {"machine": machine_config["name"], "staging_schema": staging, "status": "cleared"}


def _truncate_one_table(table: str, incremental: str, engine) -> dict:
    stats = {"table": table, "error": None}
    try:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE `{incremental}`.`{table}`"))
            conn.commit()
    except Exception as e:
        stats["error"] = str(e)
    return stats


def refresh_incremental_schema(machine_config: dict) -> dict:
    """Truncate all tables in incremental_schema (called after merge into deidentified_merged)."""
    incremental = machine_config["incremental_schema"]
    engine  = _engine()
    tables  = inspect(engine).get_table_names(schema=incremental)
    results = []

    with ThreadPoolExecutor(max_workers=TRUNCATE_MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(_truncate_one_table, t, incremental, engine): t
            for t in tables
        }
        for future in as_completed(future_to_table):
            results.append(future.result())

    engine.dispose()
    failed = [r for r in results if r["error"]]
    return {
        "machine": machine_config["name"],
        "incremental_schema": incremental,
        "tables_truncated": len(results) - len(failed),
        "failed": len(failed),
        "failed_tables": [{"table": r["table"], "error": r["error"]} for r in failed],
    }
