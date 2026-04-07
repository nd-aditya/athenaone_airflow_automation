"""
DAG: gcp_incremental_merge

Merges each machine's INCREMENTAL_SCHEMA into DEIDENTIFIED_SCHEMA using the same
3-phase logic as gcp_deid_merge_from_staging, then truncates INCREMENTAL_SCHEMA.

Steps (per machine in MACHINE_RESTORE_CONFIGS):
  1. phase_insert     — deactivate existing PKs in deid, insert new rows with nd_active_flag='Y'
  2. phase_validate   — assert exactly one active row per PK
  3. phase_fix        — correct any violations found in validate
  4. refresh_incremental — truncate INCREMENTAL_SCHEMA (ready for next append cycle)

No conf required. Machine configs are read from GCP_related/config.py.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

# ── Make services importable ──────────────────────────────────────────────────
_DAG_DIR     = os.path.dirname(__file__)                                        # GCP_related/airflow_home/dags/
_SERVICES    = os.path.abspath(os.path.join(_DAG_DIR, "..", "services"))       # GCP_related/airflow_home/services/
_GCP_RELATED = os.path.abspath(os.path.join(_DAG_DIR, "..", ".."))             # GCP_related/
for _p in (_SERVICES, _GCP_RELATED):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# ─────────────────────────────────────────────────────────────────────────────

from config import MACHINE_RESTORE_CONFIGS

with DAG(
    dag_id="gcp_incremental_merge",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "incremental", "merge", "deid"],
) as dag:

    @task
    def phase_insert_all() -> list[dict]:
        """Merge each machine's incremental_schema into DEIDENTIFIED_SCHEMA."""
        from gcp_incremental_merge_service import phase_insert
        results = []
        for mc in MACHINE_RESTORE_CONFIGS:
            results.append(phase_insert(mc))
        return results

    @task
    def phase_validate_all(insert_results: list[dict]) -> list[dict]:
        from gcp_incremental_merge_service import phase_validate
        results = []
        for insert_result in insert_results:
            results.append(phase_validate(insert_result))
        return results

    @task
    def phase_fix_all(validate_results: list[dict]) -> list[dict]:
        from gcp_incremental_merge_service import phase_fix
        results = []
        for validate_result in validate_results:
            results.append(phase_fix(validate_result))
        return results

    @task
    def refresh_incremental_all(_fix_results: list[dict]) -> list[dict]:
        """Truncate each machine's INCREMENTAL_SCHEMA after successful merge."""
        from gcp_incremental_merge_service import refresh_incremental
        results = []
        for mc in MACHINE_RESTORE_CONFIGS:
            results.append(refresh_incremental(mc, {}))
        return results

    insert_out   = phase_insert_all()
    validate_out = phase_validate_all(insert_out)
    fix_out      = phase_fix_all(validate_out)
    refresh_out  = refresh_incremental_all(fix_out)

    insert_out >> validate_out >> fix_out >> refresh_out
