"""
Merge each machine's incremental_schema → full_deidentified_schema using the same
3-phase logic as gcp_deid_merge_from_staging_service, then truncate incremental_schema.

The target schema (full_deidentified_schema) is set per machine from MACHINE_RESTORE_CONFIGS
so each provider's data is merged into its own deidentified schema.
"""
from __future__ import annotations

import os
import sys

# ── Paths ─────────────────────────────────────────────────────────────────────
_SERVICES_DIR     = os.path.dirname(__file__)                                  # GCP_related/airflow_home/services/
_GCP_RELATED_ROOT = os.path.abspath(os.path.join(_SERVICES_DIR, "..", ".."))   # GCP_related/

for _p in (_SERVICES_DIR, _GCP_RELATED_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import gcp_deid_merge_from_staging_service as _merge_svc
from gcp_restore_append_service import refresh_incremental_schema


def _set_target_schema(machine_config: dict) -> None:
    """Point the merge service at this machine's full_deidentified_schema."""
    _merge_svc.DEIDENTIFIED_SCHEMA = machine_config["full_deidentified_schema"]


def phase_insert(machine_config: dict) -> dict:
    """Merge incremental_schema into full_deidentified_schema (set N flags + insert new rows)."""
    _set_target_schema(machine_config)
    result = _merge_svc.merge_deid_staging_to_merged_phase_insert(machine_config["incremental_schema"])
    result["target_schema"] = machine_config["full_deidentified_schema"]
    return result


def phase_validate(insert_summary: dict) -> dict:
    """Validate one active row per PK in full_deidentified_schema."""
    return _merge_svc.merge_deid_staging_to_merged_phase_validate(insert_summary or {})


def phase_fix(validation_summary: dict) -> dict:
    """Fix any PK violations found during validation."""
    return _merge_svc.merge_deid_staging_to_merged_phase_fix(validation_summary or {})


def refresh_incremental(machine_config: dict, fix_summary: dict) -> dict:
    """Truncate all tables in incremental_schema after a successful merge."""
    return refresh_incremental_schema(machine_config)
