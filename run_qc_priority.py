#!/usr/bin/env python3
"""
QC report for priority tables only — prints results to terminal.
Edit the schema names below and run:
    python run_qc_priority.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# ─── SET YOUR SCHEMA NAMES HERE ───────────────────────────────────────────────
DIFF_SCHEMA = "Tng-athenaone"
DEID_SCHEMA  = "deidentified_merged"
# ──────────────────────────────────────────────────────────────────────────────

# Priority tables (mirrors TEST_TABLE_NAMES in extraction_dag.py)
PRIORITY_TABLES = {t.upper() for t in [
    "ALLERGY", "APPOINTMENT", "APPOINTMENTELIGIBILITYINFO", "APPOINTMENTNOTE",
    "APPOINTMENTVIEW", "CHART", "CHARTQUESTIONNAIRE", "CHARTQUESTIONNAIREANSWER",
    "CLINICALENCOUNTER", "CLINICALENCOUNTERDATA", "CLINICALENCOUNTERDIAGNOSIS",
    "CLINICALENCOUNTERDXICD10", "CLINICALENCOUNTERPREPNOTE", "CLINICALORDERTYPE",
    "clinicalprescription", "CLINICALRESULT", "CLINICALRESULTOBSERVATION",
    "CLINICALSERVICE", "CLINICALSERVICEPROCEDURECODE", "CLINICALTEMPLATE",
    "document", "FDB_RMIID1", "FDB_RNDC14", "ICDCODEALL", "INSURANCEPACKAGE",
    "medication", "PATIENT", "PATIENTALLERGY", "PATIENTALLERGYREACTION",
    "PATIENTFAMILYHISTORY", "PATIENTINSURANCE", "patientmedication",
    "PATIENTPASTMEDICALHISTORY", "PATIENTSOCIALHISTORY", "PATIENTSURGERY",
    "PATIENTSURGICALHISTORY", "PROCEDURECODE", "PROCEDURECODEREFERENCE",
    "SNOMED", "SOCIALHXFORMRESPONSE", "SOCIALHXFORMRESPONSEANSWER",
    "SURGICALHISTORYPROCEDURE", "visit", "VITALATTRIBUTEREADING", "VITALSIGN",
    "PROVIDER"
]}

from sqlalchemy import text, create_engine
import services.qc_service as _qc_mod
from services.qc_service import (
    MAPPING_SCHEMA, MAPPING_TABLE, DOCUMENT_TABLE,
    _col_exists, _col_has_non_null,
)
from services.config import HISTORICAL_SCHEMA, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, BRIDGE_TABLE_SCHEMA

TABLE_IDENTIFIER_MAP = {
    "CLINICALSERVICE": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "APPOINTMENTNOTE": {"ref_table": "APPOINTMENT_2", "join_col": "APPOINTMENTID", "ref_col": "patientID"},
    "CLINICALRESULTOBSERVATION": {"join_col": "DOCUMENTID", "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_clinicalresultobservation", "mapping_col": "DOCUMENTID"},
    "ALLERGY": {"col": "CHARTID"},
    "APPOINTMENT": {"join_col": "PATIENT_ID", "mapping_table": "patient_mapping_table", "mapping_col": "patientid"},
    "APPOINTMENTVIEW": {"col": "PATIENTID"},
    "CHART": {"col": "CHARTID"},
    "CHARTQUESTIONNAIRE": {"col": "CHARTID"},
    "CLINICALENCOUNTER": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "CLINICALENCOUNTERDATA": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "CLINICALENCOUNTERDIAGNOSIS": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "CLINICALENCOUNTERPREPNOTE": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "CLINICALPRESCRIPTION": {"join_col": "DOCUMENTID", "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_clinicalprescription", "mapping_col": "DOCUMENTID"},
    "CLINICALRESULT":{"join_col": "DOCUMENTID", "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_clinicalresult", "mapping_col": "DOCUMENTID"},
    "CLINICALTEMPLATE": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "DOCUMENT": {"col": "CHARTID"},
    "PATIENTMEDICATION": {"col": "CHARTID"},
    "PATIENTPASTMEDICALHISTORY": {"col": "CHARTID"},
    "PATIENTSURGERY": {"col": "CHARTID"},
    "PATIENTSURGICALHISTORY": {"col": "CHARTID"},
    "PATIENTSOCIALHISTORY": {"col": "CHARTID"},
    "SOCIALHXFORMRESPONSE": {"col": "CHARTID"},
    "SOCIALHXFORMRESPONSEANSWER": {
        "chain": [("SOCIALHXFORMRESPONSE", "SOCIALHXFORMRESPONSEID"), ("DOCUMENT", "CHARTID")],
        "col": "PATIENTID",
    },
    "VISIT": {"col": "PATIENTID"},
    "VITALSIGN": {"join_col": "clinicalencounterid", "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"},
    "VITALATTRIBUTEREADING": {"col": "CHARTID"},
}


def _engine(schema: str):
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{schema}",
        pool_pre_ping=True,
    )


def _count_from_spec(engine, schema: str, table: str, spec: dict) -> tuple:
    # Use per-entry mapping_schema if provided, otherwise fall back to default
    ms = spec.get("mapping_schema", MAPPING_SCHEMA)

    if "mapping_table" in spec:
        join_col, mapping_table, mapping_col = spec["join_col"], spec["mapping_table"], spec["mapping_col"]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(*) FROM `{schema}`.`{table}` t
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{ms}`.`{mapping_table}` m
                    WHERE m.`{mapping_col}` = t.`{join_col}`
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    if "chain" in spec:
        chain, col = spec["chain"], spec["col"]
        aliases = [f"r{i}" for i in range(len(chain))]
        joins, prev_alias = "", "t"
        for i, (join_table, join_col) in enumerate(chain):
            alias = aliases[i]
            joins += f"\nLEFT JOIN `{HISTORICAL_SCHEMA}`.`{join_table}` {alias} ON {prev_alias}.`{join_col}` = {alias}.`{join_col}`"
            prev_alias = alias
        last_alias = aliases[-1]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t{joins}
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{ms}`.`{MAPPING_TABLE}` m
                    WHERE m.`{col}` = {last_alias}.`{col}`
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    if "ref_table" in spec:
        ref_table, join_col, ref_col = spec["ref_table"], spec["join_col"], spec["ref_col"]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t
                LEFT JOIN `{HISTORICAL_SCHEMA}`.`{ref_table}` r ON t.`{join_col}` = r.`{join_col}`
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{ms}`.`{MAPPING_TABLE}` m
                    WHERE m.`{ref_col}` = r.`{ref_col}`
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    col = spec["col"]
    if not _col_has_non_null(engine, schema, table, col):
        return None, "Patient Identifier missing at source table"
    with engine.connect() as conn:
        count = conn.execute(text(f"""
            SELECT COUNT(*) FROM `{schema}`.`{table}` t
            WHERE NOT EXISTS (
                SELECT 1 FROM `{ms}`.`{MAPPING_TABLE}` m
                WHERE m.`{col}` = t.`{col}`
            )
        """)).scalar()
    return count, "Patient Identifier missing at source table"


def _ignore_row_count_v2(engine, schema: str, table: str) -> tuple:
    spec = TABLE_IDENTIFIER_MAP.get(table.upper())
    if spec is not None:
        return _count_from_spec(engine, schema, table, spec)
    for col in ("patientID", "chartID"):
        if _col_exists(engine, schema, table, col):
            if not _col_has_non_null(engine, schema, table, col):
                return None, "Patient Identifier missing at source table"
            return _count_from_spec(engine, schema, table, {"col": col})
    if _col_exists(engine, schema, table, "documentID"):
        if not _col_has_non_null(engine, schema, table, "documentID"):
            return None, "Patient Identifier missing at source table"
        return _count_from_spec(engine, schema, table, {
            "ref_table": DOCUMENT_TABLE, "join_col": "documentID", "ref_col": "patientID",
        })
    return None, "No patient identifier column in this table"


def _all_tables(engine, schema: str) -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :s ORDER BY TABLE_NAME"),
            {"s": schema},
        ).fetchall()
    return [r[0] for r in rows]


def run_qc_priority(diff_schema: str, deid_schema: str) -> None:
    orig_engine = _engine(diff_schema)
    deid_engine = _engine(deid_schema)

    all_diff_tables = _all_tables(orig_engine, diff_schema)
    priority_diff_tables = [t for t in all_diff_tables if t.upper() in PRIORITY_TABLES]
    # Case-insensitive map: uppercase name → actual name stored in deid schema
    deid_tables = {t.upper(): t for t in _all_tables(deid_engine, deid_schema)}

    rows = []
    errors = []

    for table in priority_diff_tables:
        try:
            with orig_engine.connect() as conn:
                orig_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{diff_schema}`.`{table}`")
                ).scalar() or 0

            if orig_count == 0:
                continue

            deid_actual = deid_tables.get(table.upper())
            if deid_actual is None:
                rows.append({
                    "table": table, "orig": orig_count, "deid": 0,
                    "diff": orig_count, "ignore": None,
                    "status": "FAILED", "comment": "Table not deidentified",
                })
                continue

            with deid_engine.connect() as conn:
                deid_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{deid_schema}`.`{deid_actual}`")
                ).scalar() or 0

            ignore_rows, comment = _ignore_row_count_v2(orig_engine, diff_schema, table)
            diff = orig_count - deid_count
            ignore_val = ignore_rows or 0
            status = "PASS" if abs(diff) - ignore_val == 0 else "NEED_TO_CHECK"
            rows.append({
                "table": table, "orig": orig_count, "deid": deid_count,
                "diff": diff, "ignore": ignore_rows,
                "status": status, "comment": comment if diff != 0 else "",
            })
        except Exception as e:
            errors.append({"table": table, "error": str(e)})

    orig_engine.dispose()
    deid_engine.dispose()

    # ── Terminal output ──────────────────────────────────────────────────────
    W = {"table": 36, "orig": 10, "deid": 10, "diff": 8, "ignore": 8, "status": 14, "comment": 0}
    header = (
        f"{'Table':<{W['table']}} {'Orig Cnt':>{W['orig']}} {'Deid Cnt':>{W['deid']}} "
        f"{'Diff':>{W['diff']}} {'Ignore':>{W['ignore']}} {'Status':<{W['status']}} Comment"
    )
    sep = "-" * len(header)

    print(f"\nQC: {diff_schema}  vs  {deid_schema}")
    print(sep)
    print(header)
    print(sep)

    pass_count = fail_count = failed_count = 0
    for r in rows:
        ignore_display = "N/A" if r["ignore"] is None else str(r["ignore"])
        status = r["status"]
        if status == "PASS":
            pass_count += 1
        elif status == "NEED_TO_CHECK":
            fail_count += 1
        else:
            failed_count += 1
        print(
            f"{r['table']:<{W['table']}} {r['orig']:>{W['orig']}} {r['deid']:>{W['deid']}} "
            f"{r['diff']:>{W['diff']}} {ignore_display:>{W['ignore']}} {status:<{W['status']}} {r['comment']}"
        )

    print(sep)
    print(f"  PASS: {pass_count}  |  NEED_TO_CHECK: {fail_count}  |  FAILED: {failed_count}  |  Errors: {len(errors)}")

    if errors:
        print("\nErrors:")
        for e in errors:
            print(f"  {e['table']}: {e['error']}")


if __name__ == "__main__":
    run_qc_priority(DIFF_SCHEMA, DEID_SCHEMA)
