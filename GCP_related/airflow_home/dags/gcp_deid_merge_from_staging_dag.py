"""
DAG: gcp_deid_merge_from_staging

Merges a staging deid schema into DEIDENTIFIED_SCHEMA using the 3-phase merge logic.
Triggered manually with conf specifying the staging schema.

Manual trigger conf:
  {"staging_deid_schema": "diff_04062026_deid"}   — explicit schema name (preferred)
  {"diff_schema": "diff_04062026"}                 — derives staging as diff_schema + "_deid"
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

from gcp_deid_merge_from_staging_service import (
    merge_deid_staging_to_merged_phase_fix,
    merge_deid_staging_to_merged_phase_insert,
    merge_deid_staging_to_merged_phase_validate,
)

with DAG(
    dag_id="gcp_deid_merge_from_staging",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gcp", "deid", "merge", "staging"],
) as dag:

    @task
    def resolve_staging_schema(**context) -> dict:
        """
        Read Airflow trigger config.

        Provide either:
          - conf.staging_deid_schema = "<schema name>", or
          - conf.diff_schema = "diff_YYYYMMDD" (staging becomes f"{diff_schema}_deid")
        """
        dag_run = context.get("dag_run")
        conf = dict((dag_run.conf or {}) if dag_run is not None else {})

        staging = conf.get("staging_deid_schema")
        if staging:
            return {"staging_deid_schema": staging}

        diff_schema = conf.get("diff_schema")
        if diff_schema:
            return {"staging_deid_schema": f"{diff_schema}_deid"}

        raise ValueError("Provide dag_run.conf with staging_deid_schema (preferred) or diff_schema.")

    @task
    def phase_insert(resolved: dict) -> dict:
        return merge_deid_staging_to_merged_phase_insert(resolved["staging_deid_schema"])

    @task
    def phase_validate(merge_insert_summary: dict) -> dict:
        return merge_deid_staging_to_merged_phase_validate(merge_insert_summary or {})

    @task
    def phase_fix(validation_summary: dict) -> dict:
        return merge_deid_staging_to_merged_phase_fix(validation_summary or {})

    resolved     = resolve_staging_schema()
    insert_out   = phase_insert(resolved)
    validate_out = phase_validate(insert_out)
    fix_out      = phase_fix(validate_out)

    resolved >> insert_out >> validate_out >> fix_out
