#!/usr/bin/env python3
"""
Standalone QC report runner.
Edit the schema names below and run:
    python run_qc_report.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# ─── SET YOUR SCHEMA NAMES HERE ───────────────────────────────────────────────
DIFF_SCHEMA = "diff_20260324"
DEID_SCHEMA  = "diff_20260324_deid"
# ──────────────────────────────────────────────────────────────────────────────

from sqlalchemy import text
import services.qc_service as _qc_mod
from services.qc_service import (
    MAPPING_SCHEMA, MAPPING_TABLE, DOCUMENT_TABLE,
    _col_exists, _col_has_non_null,
)
from services.config import HISTORICAL_SCHEMA
from services.email_service import send_qc_report_email

# ---------------------------------------------------------------------------
# Per-table patient-identifier spec.
# Tables NOT listed here fall back to auto-detection
# (patientID → chartID → documentID column scan).
#
# Supported forms (pick one set of keys per entry):
#
#   Direct column on the table itself:
#     {"col": "patientID"}
#
#   Single reference hop — join one historical table, then check patient col
#   against patient_mapping_table.  join_col must exist in both tables:
#     {"ref_table": "APPOINTMENT_2", "join_col": "APPOINTMENTID", "ref_col": "patientID"}
#
#   Reference chain — multiple JOIN hops in order, final col checked against
#   patient_mapping_table:
#     {"chain": [("CLINICALRESULT", "CLINICALRESULTID"), ("DOCUMENT", "DOCUMENTID")],
#      "col": "patientID"}
#
#   Custom mapping table — join directly against a non-standard mapping table:
#     {"join_col": "clinicalencounterid",
#      "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"}
# ---------------------------------------------------------------------------
TABLE_IDENTIFIER_MAP = {
    "CLINICALSERVICE": {
        "join_col": "clinicalencounterid",
        "mapping_table": "encounter_mapping_table",
        "mapping_col": "encounter_id",
    },
    "APPOINTMENTNOTE": {
        "ref_table": "APPOINTMENT_2",
        "join_col": "APPOINTMENTID",
        "ref_col": "patientID",
    },
    "CLINICALRESULTOBSERVATION": {
        "chain": [("CLINICALRESULT", "CLINICALRESULTID"), ("DOCUMENT", "DOCUMENTID")],
        "col": "patientID",
    },
}


def _count_from_spec(engine, schema: str, table: str, spec: dict) -> tuple:
    """Build and run the ignore-row count query for a given spec."""

    # ── Custom mapping table ────────────────────────────────────────────────
    if "mapping_table" in spec:
        join_col      = spec["join_col"]
        mapping_table = spec["mapping_table"]
        mapping_col   = spec["mapping_col"]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(*) FROM `{schema}`.`{table}` t
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`{mapping_table}` m
                    WHERE m.`{mapping_col}` = t.`{join_col}`
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    # ── Reference chain (multi-hop) ─────────────────────────────────────────
    if "chain" in spec:
        chain = spec["chain"]   # list of (table_name, join_col) tuples
        col   = spec["col"]
        aliases = [f"r{i}" for i in range(len(chain))]
        joins = ""
        prev_alias = "t"
        for i, (join_table, join_col) in enumerate(chain):
            alias  = aliases[i]
            joins += (
                f"\nJOIN `{HISTORICAL_SCHEMA}`.`{join_table}` {alias} "
                f"ON {prev_alias}.`{join_col}` = {alias}.`{join_col}`"
            )
            prev_alias = alias
        last_alias = aliases[-1]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t{joins}
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                    WHERE m.`{col}` = {last_alias}.`{col}`
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    # ── Single reference hop ────────────────────────────────────────────────
    if "ref_table" in spec:
        ref_table = spec["ref_table"]
        join_col  = spec["join_col"]
        ref_col   = spec["ref_col"]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t
                JOIN `{HISTORICAL_SCHEMA}`.`{ref_table}` r ON t.`{join_col}` = r.`{join_col}`
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                    WHERE m.`{ref_col}` = r.`{ref_col}`
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    # ── Direct column ───────────────────────────────────────────────────────
    col = spec["col"]
    if not _col_has_non_null(engine, schema, table, col):
        return None, "Patient Identifier missing at source table"
    with engine.connect() as conn:
        count = conn.execute(text(f"""
            SELECT COUNT(*) FROM `{schema}`.`{table}` t
            WHERE NOT EXISTS (
                SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                WHERE m.`{col}` = t.`{col}`
            )
        """)).scalar()
    return count, "Patient Identifier missing at source table"


def _ignore_row_count_v2(engine, schema: str, table: str) -> tuple:
    """
    Map-first ignore-row count.
    Checks TABLE_IDENTIFIER_MAP first; falls back to column auto-detection
    for tables not listed in the map.
    """
    spec = TABLE_IDENTIFIER_MAP.get(table.upper())
    if spec is not None:
        return _count_from_spec(engine, schema, table, spec)

    # ── Auto-detection fallback ─────────────────────────────────────────────
    for col in ("patientID", "chartID"):
        if _col_exists(engine, schema, table, col):
            if not _col_has_non_null(engine, schema, table, col):
                return None, "Patient Identifier missing at source table"
            return _count_from_spec(engine, schema, table, {"col": col})

    if _col_exists(engine, schema, table, "documentID"):
        if not _col_has_non_null(engine, schema, table, "documentID"):
            return None, "Patient Identifier missing at source table"
        return _count_from_spec(engine, schema, table, {
            "ref_table": DOCUMENT_TABLE,
            "join_col":  "documentID",
            "ref_col":   "patientID",
        })

    return None, "No patient identifier column in this table"


# Patch the service module so run_qc uses the new logic
_qc_mod._ignore_row_count = _ignore_row_count_v2

from services.qc_service import run_qc   # import AFTER patching


if __name__ == "__main__":
    print(f"Running QC: {DIFF_SCHEMA}  vs  {DEID_SCHEMA}")
    result = run_qc(DIFF_SCHEMA, DEID_SCHEMA)
    print(
        f"  PASS: {result['pass_count']}  |  "
        f"NEED_TO_CHECK: {result['fail_count']}  |  "
        f"FAILED: {result['failed_count']}  |  "
        f"Errors: {len(result['errors'])}"
    )

    sent = send_qc_report_email(result)
    if sent:
        print("Email sent successfully.")
    else:
        print("Email failed — check EMAIL_SENDER / EMAIL_APP_PASSWORD in services/config.py")
