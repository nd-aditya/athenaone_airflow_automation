#!/usr/bin/env python3
"""
find_differences_v5.py
------------------------------------------------
Compare two SQL Server dumps (FIRST_DUMP_CONN_STR, SECOND_DUMP_CONN_STR)
Find all rows present in SECOND but not in FIRST for each table (by columns)
and insert those "diff" rows into DIFF_CONN_STR (MySQL).
"""

import os, json, time, hashlib, traceback, urllib.parse
from datetime import datetime
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text

# -------- CONFIGURATION: Edit for your dumps/targets ----------------------

FIRST_DUMP_TYPE = "mssql"
FIRST_DUMP_CONN_STR = "mssql+pymssql://sa:ndADMIN2025@localhost:1433/mobiledoc"
SECOND_DUMP_TYPE = "mssql"
SECOND_DUMP_CONN_STR = "mssql+pymssql://sa:ndADMIN2025@localhost:1433/mobiledoc_23102025"

DIFF_CONN_STR = "mysql+mysqlconnector://ndadmin:ndADMIN%402025@localhost:3306/mobiledoc_feb_oct_diff"

CHUNK_SIZE = 250000
HASH_BUCKETS = 64
MAX_WORKERS = 4

# -------------------------------------------------------------------------

def log(msg):
    now = datetime.now().strftime('%H:%M:%S')
    print(f"[{now}] {msg}", flush=True)

def mssql_engine(conn_str):
    """Create engine for MSSQL using pymssql driver.
    This avoids the pyodbc/ODBC error!"""
    try:
        eng = create_engine(conn_str, pool_pre_ping=True)
        # quick check: try to connect to fail fast
        with eng.connect() as conn:
            pass
        return eng
    except Exception as e:
        log(f"❌ Error connecting to MSSQL: {e}")
        raise

def mysql_engine(conn_url):
    try:
        eng = create_engine(conn_url, pool_pre_ping=True)
        with eng.connect() as conn:
            pass
        return eng
    except Exception as e:
        log(f"❌ Error connecting to MySQL: {e}")
        raise

def get_table_list(engine, db_type):
    if db_type == "mssql":
        q = text("SELECT name FROM sys.tables WHERE is_ms_shipped=0 ORDER BY name")
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
            return [r[0] for r in rows]
    else:
        df = pd.read_sql(text("SHOW TABLES"), engine)
        return df.iloc[:, 0].tolist()

def get_columns(engine, db_type, table):
    if db_type == "mssql":
        q = text(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=:t ORDER BY ORDINAL_POSITION")
        with engine.connect() as conn:
            rows = conn.execute(q, {"t": table}).fetchall()
            return [r[0] for r in rows]
    else:
        df = pd.read_sql(text(f"SHOW COLUMNS FROM `{table}`"), engine)
        return df["Field"].tolist()

def get_primary_key(engine, db_type, table):
    if db_type == "mssql":
        q = text("""
            SELECT c.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
            ON t.CONSTRAINT_NAME = c.CONSTRAINT_NAME
            WHERE t.TABLE_NAME=:table AND t.CONSTRAINT_TYPE='PRIMARY KEY'
        """)
        with engine.connect() as conn:
            row = conn.execute(q, {"table": table}).fetchone()
            return row[0] if row else None
    else:
        q = text("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table AND COLUMN_KEY='PRI' LIMIT 1")
        df = pd.read_sql(q, engine, params={"table": table})
        return df.iloc[0,0] if not df.empty else None

def ensure_diff_table(diff_engine, table, columns):
    col_types = []
    for c in columns:
        t = "TEXT"
        col_types.append(f"`{c}` {t}")
    col_types.append("`difference_created_date` DATETIME")
    with diff_engine.connect() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(col_types)})"
        ))

def insert_diff_rows(diff_engine, table, df):
    if df.empty: return 0
    df = df.copy()
    df['difference_created_date'] = datetime.now()
    try:
        df.to_sql(table, diff_engine, if_exists="append", index=False, method="multi")
        return len(df)
    except Exception as e:
        log(f"❌ Failed to insert diff rows for {table}: {e}")
        return 0

def md5_df(df):
    if df.empty: return ""
    return hashlib.md5(df.to_csv(index=False).encode("utf-8")).hexdigest()

def compare_tables(table, eng1, eng2, diff_engine):
    # Get columns and common columns
    cols1 = get_columns(eng1, FIRST_DUMP_TYPE, table)
    cols2 = get_columns(eng2, SECOND_DUMP_TYPE, table)
    common_cols = [c for c in cols1 if c in cols2]
    if not common_cols:
        log(f"⚠️ Skipping {table}: no common columns.")
        return 0

    pkey = get_primary_key(eng2, SECOND_DUMP_TYPE, table)
    log(f"Comparing: {table} (PK={pkey}, common_cols={len(common_cols)})")

    # Chunking (if any) only if primary key appears numeric (for now, we just do full-table for simplicity!)
    # --- FULL TABLE (middle-size tables only)
    try:
        # Load second dump (target) in batches for memory
        q = f"SELECT {', '.join([f'[{c}]' for c in common_cols])} FROM [{table}]"
        df2 = pd.read_sql(q, eng2)
        if df2.empty:
            log(f"Table {table}: second dump empty; skipping diff.")
            return 0
        # Load first dump for the same columns
        q1 = f"SELECT {', '.join([f'[{c}]' for c in common_cols])} FROM [{table}]"
        df1 = pd.read_sql(q1, eng1)
        # Compute diff: rows present in df2, not in df1 (by all columns)
        merge = df2.merge(df1, how="left", on=common_cols, indicator=True)
        diff = merge[merge["_merge"]=="left_only"].drop(columns=["_merge"])
        if diff.empty:
            log(f"✅ {table}: No diffs found.")
            return 0
        ensure_diff_table(diff_engine, table, diff.columns)
        count = insert_diff_rows(diff_engine, table, diff)
        log(f"✅ {table}: {count} diff rows stored.")
        return count
    except Exception as e:
        log(f"❌ Error diffing {table}: {e}")
        return 0

def main():
    log("🚀 Starting MSSQL Dump Difference Extractor (FIRST vs SECOND), results to DIFF DB")

    if not FIRST_DUMP_CONN_STR or not SECOND_DUMP_CONN_STR:
        log("❗ Please set FIRST_DUMP_CONN_STR and SECOND_DUMP_CONN_STR in this script!")
        exit(1)

    eng1 = mssql_engine(FIRST_DUMP_CONN_STR)
    eng2 = mssql_engine(SECOND_DUMP_CONN_STR)
    diff_engine = mysql_engine(DIFF_CONN_STR)

    all_tables = get_table_list(eng2, SECOND_DUMP_TYPE)
    log(f"Tables to process (from 2nd dump): {len(all_tables)}\n")

    summary = []
    for table in tqdm(all_tables, desc="Tables", unit="tbl"):
        try:
            n = compare_tables(table, eng1, eng2, diff_engine)
            summary.append((table, n))
        except Exception as e:
            log(f"❌ Error in {table}: {e}")
            summary.append((table, "ERROR"))

    total_diffrows = sum((x[1] if isinstance(x[1], int) else 0) for x in summary)
    log("="*60)
    log(f"All done! {total_diffrows} total diff rows inserted for {len([s for s in summary if s[1] and isinstance(s[1], int) and s[1]>0])} tables.")

if __name__ == "__main__":
    main()
