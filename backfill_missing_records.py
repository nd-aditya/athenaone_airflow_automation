#!/usr/bin/env python3
"""
One-time backfill: extract records from Snowflake that are missing in the MySQL
historical schema, identified by primary key.

For each table:
  1.  Creates a MySQL temp table with the existing distinct PK values from historical.
  2.  If PKs fit within NOT_IN_LIMIT, passes them directly to Snowflake as NOT IN.
      Otherwise pulls all Snowflake rows and filters missing PKs in Python.
  3.  Inserts the missing records into MySQL historical schema.
  4.  Drops the temp table.

PKs are loaded from table_primary_keys.csv (pipe-separated for composite keys).
CONTEXTID is excluded from NOT IN — it is already covered by the contextid filter.

Run:
    python backfill_missing_records.py
"""
import csv
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from sqlalchemy import create_engine, text

from services.config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE,
    MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST,
    HISTORICAL_SCHEMA, INCREMENTAL_SCHEMA,
    CONTEXT_IDS,
)
from services.extraction_service import data_type_mapping

# ── CONFIG ────────────────────────────────────────────────────────────────────
EXTRACT_SOURCE_CONFIGS = [
    {"schema": "ATHENAONE",  "table_rename_map": {"APPOINTMENT": "appointment_2"}, "context_filter": True},
    {"schema": "SCHEDULING", "table_rename_map": {}, "context_filter": False},
    {"schema": "FINANCIALS",  "table_rename_map": {}, "context_filter": False},
]

# Leave empty [] to process ALL tables, or list specific MySQL table names to limit scope
TARGET_TABLES = []  # e.g. ["PATIENT", "appointment_2"]

# Max PKs passed via SQL NOT IN.  Above this, Python-side set comparison is used.
NOT_IN_LIMIT = 50_000

# Rows fetched from Snowflake per iteration
SF_FETCH_SIZE = 10_000

# Rows inserted into MySQL per batch
MYSQL_BATCH_SIZE = 5_000

PK_CSV = os.path.join(os.path.dirname(__file__), "table_primary_keys.csv")
# ─────────────────────────────────────────────────────────────────────────────


# ── Engine helpers ─────────────────────────────────────────────────────────────

def _sf_engine(schema: str):
    return create_engine(
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
        f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{schema}"
        f"?warehouse={SNOWFLAKE_WAREHOUSE}",
        connect_args={"insecure_mode": True},
        pool_pre_ping=True,
    )


def _mysql_engine(schema: str):
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{schema}",
        pool_pre_ping=True,
    )


# ── PK map ─────────────────────────────────────────────────────────────────────

def _load_pk_map() -> dict:
    """Returns {TABLE_NAME_UPPER: [col1, col2, ...]} from table_primary_keys.csv."""
    pk_map = {}
    try:
        with open(PK_CSV, newline="") as f:
            for row in csv.DictReader(f):
                table = row["table_name"].strip().upper()
                cols  = [c.strip() for c in row["primary_key"].split("|") if c.strip()]
                pk_map[table] = cols
    except FileNotFoundError:
        print(f"WARNING: {PK_CSV} not found — tables without PKs will be skipped.")
    return pk_map


def _filter_pk_cols(pk_cols: list) -> list:
    """Remove CONTEXTID from PK cols — contextid is already handled by the contextid filter."""
    return [c for c in pk_cols if c.upper() != "CONTEXTID"]


# ── MySQL helpers ──────────────────────────────────────────────────────────────

def _table_exists_mysql(engine, table: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t LIMIT 1"
        ), {"s": HISTORICAL_SCHEMA, "t": table}).scalar() is not None


def _create_temp_pk_table(engine, mysql_table: str, pk_cols: list) -> str:
    """
    Create a MySQL temp table _pk_tmp_<table> containing the distinct PK values
    that already exist in the historical schema.  Returns the temp table name.
    """
    tmp_name = f"_pk_tmp_{mysql_table}"
    cols_expr = ", ".join(f"`{c}`" for c in pk_cols)
    with engine.connect() as conn:
        conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{tmp_name}`"))
        conn.execute(text(
            f"CREATE TEMPORARY TABLE `{tmp_name}` AS "
            f"SELECT DISTINCT {cols_expr} FROM `{HISTORICAL_SCHEMA}`.`{mysql_table}`"
        ))
        conn.commit()
    return tmp_name


def _load_existing_pks(engine, tmp_name: str, pk_cols: list) -> set:
    """Load the temp table's PK values into a Python set for in-memory filtering."""
    with engine.connect() as conn:
        cols_expr = ", ".join(f"`{c}`" for c in pk_cols)
        rows = conn.execute(text(
            f"SELECT {cols_expr} FROM `{tmp_name}`"
        )).fetchall()
    if len(pk_cols) == 1:
        return {r[0] for r in rows}
    return {tuple(r) for r in rows}


def _drop_temp_pk_table(engine, tmp_name: str):
    with engine.connect() as conn:
        conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{tmp_name}`"))
        conn.commit()


# ── Snowflake helpers ──────────────────────────────────────────────────────────

def _sf_columns(sf_engine, sf_schema: str, sf_table: str) -> pd.DataFrame:
    """Returns DESC VIEW result as DataFrame with columns [name, type, ...]."""
    with sf_engine.connect() as conn:
        result = conn.execute(text(
            f"DESC VIEW {SNOWFLAKE_DATABASE}.{sf_schema}.{sf_table}"
        ))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


def _build_context_filter(context_filter: bool) -> str:
    if context_filter:
        return f"contextid IN {CONTEXT_IDS}"
    return "1=1"


def _build_not_in_clause(pk_cols: list, existing_pks: set) -> str:
    """Build SQL NOT IN clause for Snowflake from the existing PK set."""
    if not existing_pks:
        return ""

    if len(pk_cols) == 1:
        col = pk_cols[0]
        vals = ", ".join(
            f"'{v}'" if isinstance(v, str) else str(v)
            for v in existing_pks
        )
        return f"AND {col} NOT IN ({vals})"

    # Composite PK: (col1, col2) NOT IN ((v1a, v1b), (v2a, v2b), ...)
    cols_expr = ", ".join(pk_cols)
    rows_expr = ", ".join(
        "(" + ", ".join(
            f"'{v}'" if isinstance(v, str) else str(v)
            for v in (row if isinstance(row, tuple) else (row,))
        ) + ")"
        for row in existing_pks
    )
    return f"AND ({cols_expr}) NOT IN ({rows_expr})"


# ── Insertion helpers ──────────────────────────────────────────────────────────

def _ensure_table(inc_engine, mysql_table: str, sf_engine, sf_schema: str, sf_table: str):
    """Create the table in incremental schema from Snowflake schema if it doesn't exist yet."""
    with inc_engine.connect() as conn:
        exists = conn.execute(text(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t LIMIT 1"
        ), {"s": INCREMENTAL_SCHEMA, "t": mysql_table}).scalar() is not None
    if exists:
        return
    col_df = _sf_columns(sf_engine, sf_schema, sf_table)
    col_defs = []
    for _, row in col_df.iterrows():
        sf_type    = str(row.get("type", "TEXT")).strip()
        mysql_type = data_type_mapping.get(sf_type, "TEXT")
        col_defs.append(f"`{row['name']}` {mysql_type}")
    col_defs.append("`nd_extracted_Date` DATE")
    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{INCREMENTAL_SCHEMA}`.`{mysql_table}` "
        f"({', '.join(col_defs)})"
    )
    with inc_engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    print(f"    Created table {mysql_table} in incremental schema.")


def _insert_batch(inc_engine, mysql_table: str, df: pd.DataFrame):
    df = df.copy()
    df["nd_extracted_Date"] = pd.Timestamp.now().date()
    df.to_sql(
        mysql_table,
        con=inc_engine,
        schema=INCREMENTAL_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=MYSQL_BATCH_SIZE,
        method="multi",
    )


# ── Per-table backfill ─────────────────────────────────────────────────────────

def _backfill_table(
    sf_engine, hist_engine, inc_engine,
    sf_schema: str, sf_table: str, mysql_table: str,
    pk_cols: list, context_filter: bool,
) -> dict:
    """
    Backfill one table.  Returns summary dict.
    hist_engine: reads existing PKs from historical schema (source of truth).
    inc_engine:  inserts new records into incremental schema.
    """
    non_ctx_pks = _filter_pk_cols(pk_cols)

    # Ensure table exists in incremental schema
    _ensure_table(inc_engine, mysql_table, sf_engine, sf_schema, sf_table)

    # Step 1 — create temp table in historical and load existing PKs
    if non_ctx_pks and _table_exists_mysql(hist_engine, mysql_table):
        tmp_name     = _create_temp_pk_table(hist_engine, mysql_table, non_ctx_pks)
        existing_pks = _load_existing_pks(hist_engine, tmp_name, non_ctx_pks)
    else:
        tmp_name     = None
        existing_pks = set()

    ctx_clause = _build_context_filter(context_filter)

    # Step 2 — build Snowflake query
    if non_ctx_pks and len(existing_pks) <= NOT_IN_LIMIT:
        # SQL NOT IN — Snowflake does the filtering
        not_in = _build_not_in_clause(non_ctx_pks, existing_pks)
        sf_query = (
            f"SELECT * FROM {SNOWFLAKE_DATABASE}.{sf_schema}.{sf_table} "
            f"WHERE {ctx_clause} {not_in}"
        )
        use_python_filter = False
    else:
        # Too many PKs — pull all rows and filter in Python
        sf_query = (
            f"SELECT * FROM {SNOWFLAKE_DATABASE}.{sf_schema}.{sf_table} "
            f"WHERE {ctx_clause}"
        )
        use_python_filter = bool(non_ctx_pks)
        if use_python_filter:
            print(f"    {mysql_table}: {len(existing_pks):,} existing PKs — using Python-side filtering.")

    # Step 3 — stream from Snowflake and insert
    inserted = 0
    with sf_engine.connect().execution_options(stream_results=True) as conn:
        result = conn.execute(text(sf_query))
        columns = list(result.keys())
        while True:
            rows = result.fetchmany(SF_FETCH_SIZE)
            if not rows:
                break
            df = pd.DataFrame(rows, columns=columns)

            if use_python_filter:
                if len(non_ctx_pks) == 1:
                    col = non_ctx_pks[0]
                    df = df[~df[col].isin(existing_pks)]
                else:
                    mask = df.apply(
                        lambda r: tuple(r[c] for c in non_ctx_pks) not in existing_pks,
                        axis=1,
                    )
                    df = df[mask]

            if df.empty:
                continue
            _insert_batch(inc_engine, mysql_table, df)
            inserted += len(df)
            print(f"    {mysql_table}: inserted {inserted:,} rows so far...")

    # Step 4 — drop temp table
    if tmp_name:
        _drop_temp_pk_table(hist_engine, tmp_name)

    return {"table": mysql_table, "inserted": inserted}


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    pk_map      = _load_pk_map()
    hist_engine = _mysql_engine(HISTORICAL_SCHEMA)
    inc_engine  = _mysql_engine(INCREMENTAL_SCHEMA)

    target_upper = {t.upper() for t in TARGET_TABLES} if TARGET_TABLES else None

    grand_inserted = 0
    skipped        = []

    for cfg in EXTRACT_SOURCE_CONFIGS:
        sf_schema      = cfg["schema"]
        rename_map     = {k.upper(): v for k, v in cfg.get("table_rename_map", {}).items()}
        context_filter = cfg.get("context_filter", False)

        print(f"\n{'='*70}")
        print(f"  Schema: {sf_schema}")
        print(f"{'='*70}")

        try:
            sf_eng = _sf_engine(sf_schema)
            with sf_eng.connect() as conn:
                sf_tables = [
                    r[0] for r in conn.execute(text(
                        f"SELECT TABLE_NAME FROM {SNOWFLAKE_DATABASE}.INFORMATION_SCHEMA.TABLES "
                        f"WHERE TABLE_SCHEMA = '{sf_schema.upper()}' ORDER BY TABLE_NAME"
                    )).fetchall()
                ]
        except Exception as e:
            print(f"  ERROR connecting to Snowflake/{sf_schema}: {e}")
            continue

        for sf_table in sf_tables:
            mysql_table = rename_map.get(sf_table.upper(), sf_table)

            # Apply TARGET_TABLES filter
            if target_upper and mysql_table.upper() not in target_upper:
                continue

            pk_cols = pk_map.get(sf_table.upper()) or pk_map.get(mysql_table.upper())
            if not pk_cols:
                print(f"  SKIP {mysql_table}: no PK defined in {os.path.basename(PK_CSV)}")
                skipped.append(mysql_table)
                continue

            print(f"\n  Processing {mysql_table} (PK: {' | '.join(pk_cols)}) ...")
            try:
                summary = _backfill_table(
                    sf_eng, hist_engine, inc_engine,
                    sf_schema, sf_table, mysql_table,
                    pk_cols, context_filter,
                )
                grand_inserted += summary["inserted"]
                print(f"  DONE  {mysql_table}: {summary['inserted']:,} new rows inserted.")
            except Exception as e:
                print(f"  ERROR {mysql_table}: {e}")

    print(f"\n{'='*70}")
    print(f"  COMPLETE — Total new rows inserted: {grand_inserted:,}")
    if skipped:
        print(f"  Skipped (no PK): {', '.join(skipped)}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    run()
