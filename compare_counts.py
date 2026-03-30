#!/usr/bin/env python3
"""
Snowflake vs MySQL historical schema row count comparison.

For each table in the three Snowflake source schemas it prints:
  SF_COUNT      - COUNT(*) from Snowflake view
  MYSQL_TOTAL   - COUNT(*) from MySQL historical (all versions)
  MYSQL_ACTIVE  - COUNT(*) WHERE nd_active_flag = 'Y'  (current state)
  MYSQL_DIST_PK - COUNT(DISTINCT <primary key>) in MySQL historical
  MATCH         - OK when SF_COUNT == MYSQL_ACTIVE, DIFF otherwise

Run:
    python compare_counts.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text

from services.config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
    MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, HISTORICAL_SCHEMA,
)

# ── Same as extraction_dag.py ──────────────────────────────────────────────────
EXTRACT_SOURCE_CONFIGS = [
    {"schema": "ATHENAONE",  "table_rename_map": {"APPOINTMENT": "appointment_2"}},
    {"schema": "SCHEDULING", "table_rename_map": {}},
    {"schema": "FINANCIALS", "table_rename_map": {}},
]

# Set to True to only compare priority tables (same list as DAG 2 / run_qc_priority.py)
PRIORITY_TABLES_ONLY = False

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
    "PROVIDER", "PROVIDERGROUP",
]}
# ──────────────────────────────────────────────────────────────────────────────

# Column widths for terminal output
_W = [40, 12, 14, 14, 14, 6]
_HEADERS = ["TABLE (mysql)", "SF_COUNT", "MYSQL_TOTAL", "MYSQL_ACTIVE", "MYSQL_DIST_PK", "MATCH"]


# ── Engine helpers ─────────────────────────────────────────────────────────────

def _sf_engine(schema: str):
    return create_engine(
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
        f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{schema}"
        f"?warehouse={SNOWFLAKE_WAREHOUSE}",
        connect_args={"insecure_mode": True},
        pool_pre_ping=True,
    )


def _mysql_engine():
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{HISTORICAL_SCHEMA}",
        pool_pre_ping=True,
    )


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def _sf_tables(engine, schema: str) -> list:
    """All view/table names in the given Snowflake schema."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT TABLE_NAME "
            f"FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema.upper()}' "
            f"ORDER BY TABLE_NAME"
        )).fetchall()
    return [r[0] for r in rows]


def _sf_count(engine, schema: str, table: str):
    try:
        with engine.connect() as conn:
            return conn.execute(text(
                f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{schema}.{table}"
            )).scalar()
    except Exception as e:
        return f"ERR({e})"


# ── MySQL helpers ──────────────────────────────────────────────────────────────

def _mysql_table_exists(engine, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t LIMIT 1"
        ), {"s": HISTORICAL_SCHEMA, "t": table}).scalar() is not None


def _pk_columns(engine, table: str) -> list:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t "
            "AND CONSTRAINT_NAME = 'PRIMARY' "
            "ORDER BY ORDINAL_POSITION"
        ), {"s": HISTORICAL_SCHEMA, "t": table}).fetchall()
    return [r[0] for r in rows]


def _mysql_counts(engine, table: str):
    """Returns (total, active, dist_pk). Each value is an int, 'N/A', 'NO PK', or 'ERR'."""
    if not _mysql_table_exists(engine, table):
        return None, None, None

    with engine.connect() as conn:
        total = conn.execute(text(
            f"SELECT COUNT(*) FROM `{HISTORICAL_SCHEMA}`.`{table}`"
        )).scalar()

        try:
            active = conn.execute(text(
                f"SELECT COUNT(*) FROM `{HISTORICAL_SCHEMA}`.`{table}` "
                f"WHERE nd_active_flag = 'Y'"
            )).scalar()
        except Exception:
            active = "N/A"

        pk_cols = _pk_columns(engine, table)
        if not pk_cols:
            dist_pk = "NO PK"
        else:
            pk_expr = (
                f"`{pk_cols[0]}`" if len(pk_cols) == 1
                else "CONCAT_WS(',', " + ", ".join(f"`{c}`" for c in pk_cols) + ")"
            )
            try:
                dist_pk = conn.execute(text(
                    f"SELECT COUNT(DISTINCT {pk_expr}) FROM `{HISTORICAL_SCHEMA}`.`{table}`"
                )).scalar()
            except Exception:
                dist_pk = "ERR"

    return total, active, dist_pk


# ── Output helpers ─────────────────────────────────────────────────────────────

def _fmt(val, width, right_align=True):
    s = "-" if val is None else str(val)
    return s.rjust(width) if right_align else s.ljust(width)


def _print_row(values):
    parts = [_fmt(v, _W[i], right_align=(i > 0)) for i, v in enumerate(values)]
    print("  ".join(parts))


def _separator():
    print("  " + "  ".join("-" * w for w in _W))


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    mysql_engine = _mysql_engine()

    grand_total = grand_match = grand_diff = 0

    for cfg in EXTRACT_SOURCE_CONFIGS:
        sf_schema  = cfg["schema"]
        rename_map = {k.upper(): v for k, v in cfg.get("table_rename_map", {}).items()}

        mode = " [PRIORITY TABLES ONLY]" if PRIORITY_TABLES_ONLY else ""
        print(f"\n{'='*96}")
        print(f"  Snowflake schema: {sf_schema}  →  MySQL historical: {HISTORICAL_SCHEMA}{mode}")
        print(f"{'='*96}")
        _print_row(_HEADERS)
        _separator()

        try:
            sf_eng = _sf_engine(sf_schema)
            tables = _sf_tables(sf_eng, sf_schema)
            if PRIORITY_TABLES_ONLY:
                tables = [t for t in tables if t.upper() in PRIORITY_TABLES]
        except Exception as e:
            print(f"  ERROR connecting to Snowflake/{sf_schema}: {e}")
            continue

        schema_match = schema_diff = 0

        for sf_table in tables:
            mysql_table = rename_map.get(sf_table.upper(), sf_table)

            sf_cnt   = _sf_count(sf_eng, sf_schema, sf_table)
            total, active, dist_pk = _mysql_counts(mysql_engine, mysql_table)

            if isinstance(sf_cnt, int) and isinstance(active, int):
                match = "OK" if sf_cnt == active else "DIFF"
                if match == "OK":
                    schema_match += 1
                else:
                    schema_diff += 1
            else:
                match = "?"

            _print_row([
                mysql_table,
                sf_cnt   if sf_cnt  is not None else "MISSING",
                total    if total   is not None else "MISSING",
                active   if active  is not None else "MISSING",
                dist_pk  if dist_pk is not None else "MISSING",
                match,
            ])

        _separator()
        print(f"  Tables: {len(tables)}   OK: {schema_match}   DIFF: {schema_diff}")
        grand_total += len(tables)
        grand_match += schema_match
        grand_diff  += schema_diff

    print(f"\n{'='*96}")
    print(f"  GRAND TOTAL — Tables: {grand_total}   OK: {grand_match}   DIFF: {grand_diff}")
    print(f"{'='*96}\n")


if __name__ == "__main__":
    run()
