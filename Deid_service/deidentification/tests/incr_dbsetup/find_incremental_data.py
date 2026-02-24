#!/usr/bin/env python3
"""
find_differences_v5.py (with progress resume + no common col flag, datatype normalization aware)
------------------------------------------------
Compare two SQL Server/MySQL dumps (OLDER_DUMP_CONN_STR, NEWER_DUMP_CONN_STR)
Find rows present in NEWER but not in OLDER for each table,
and insert those rows into DIFF_CONN_STR (MySQL).

Supports checkpointing using progress.json so script can resume.

!!! NOTE: Make sure OLDER_DUMP_TYPE and NEWER_DUMP_TYPE match the engine you are connecting to. !!!
"""

import os, json, time, hashlib, traceback, urllib.parse
from datetime import datetime, date
import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import create_engine, text

# -------- CONFIGURATION ----------------------

OLDER_DUMP_TYPE = "mysql"  # Should be 'mssql' or 'mysql'
OLDER_DUMP_CONN_STR = "mysql+pymysql://root:123456789@localhost:3306/automation_incr_testing"
NEWER_DUMP_TYPE = "mssql"  # Should be 'mssql' or 'mysql'
NEWER_DUMP_CONN_STR = "mssql+pymssql://sa:ndAdmin2025@localhost:1433/automation_incr_testing"

DIFF_DB_NAME = "automation_incr_testing_diff_28nov2025"
DIFF_DUMP_CONN_STR = f"mysql+mysqlconnector://root:123456789@localhost:3306/{DIFF_DB_NAME}"

CHUNK_SIZE = 250000
HASH_BUCKETS = 64
MAX_WORKERS = 4

PROGRESS_FILE = "progress_diff.json"

EXTRA_COLUMNS = ["nd_extracted_date", "nd_auto_increment_id"]

# --------------------------------------------------

def log(msg):
    now = datetime.now().strftime('%H:%M:%S')
    print(f"[{now}] {msg}", flush=True)

# ---------------- Progress Handling ---------------- #

def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {}
    try:
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

# --------------- Create MySQL Database if Not Exists ---------------

def ensure_mysql_database_exists(conn_str, db_name):
    import sqlalchemy.engine.url
    url_obj = sqlalchemy.engine.make_url(conn_str)
    # Remove database part so we can connect to the server, not the DB
    url_no_db = url_obj.set(database=None)
    engine = create_engine(url_no_db, pool_pre_ping=True)
    with engine.connect() as conn:
        existing = conn.execute(text(
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :name"
        ), {"name": db_name}).fetchone()
        if not existing:
            log(f"Creating MySQL database '{db_name}' as it does not exist...")
            conn.execute(text(f"CREATE DATABASE `{db_name}` DEFAULT CHARACTER SET utf8mb4"))
        else:
            log(f"MySQL database '{db_name}' already exists.")

# ---------------- Database Helpers ---------------- #

def mssql_engine(conn_str):
    try:
        eng = create_engine(conn_str, pool_pre_ping=True)
        with eng.connect():
            pass
        return eng
    except Exception as e:
        log(f"❌ Error connecting to MSSQL: {e}")
        raise

def mysql_engine(conn_url):
    try:
        eng = create_engine(conn_url, pool_pre_ping=True)
        with eng.connect():
            pass
        return eng
    except Exception as e:
        log(f"❌ Error connecting to MySQL: {e}")
        raise

def get_table_list(engine, db_type):
    # Note: The type must be EXACTLY "mssql" or "mysql"
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
        q = text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = :t
            ORDER BY ORDINAL_POSITION
        """)
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
            WHERE t.TABLE_NAME = :table
              AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
        """)
        with engine.connect() as conn:
            row = conn.execute(q, {"table": table}).fetchone()
            return row[0] if row else None
    else:
        q = text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table
              AND COLUMN_KEY = 'PRI'
            LIMIT 1
        """)
        df = pd.read_sql(q, engine, params={"table": table})
        return df.iloc[0, 0] if not df.empty else None

def ensure_diff_table(diff_engine, table, columns):
    col_types = [f"`{c}` TEXT" for c in columns]
    # Change nd_extracted_date to DATE type instead of DATETIME
    col_types.append("`nd_extracted_date` DATE")
    with diff_engine.connect() as conn:
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS `{table}` ({', '.join(col_types)})"
        ))

def insert_diff_rows(diff_engine, table, df):
    if df.empty:
        return 0
    df = df.copy()
    # Insert only date part (not datetime) for nd_extracted_date
    df['nd_extracted_date'] = date.today()
    try:
        df.to_sql(table, diff_engine, if_exists="append", index=False, method="multi")
        return len(df)
    except Exception as e:
        log(f"❌ Failed to insert diff rows for {table}: {e}")
        return 0

# ---------- Data Normalization Helpers ----------

def normalize_column(col, series):
    """
    Normalize the series for fair comparison across SQL Server/MySQL differences in types.
    - For datetime types: strip microseconds/trailing zeros for fair comparison
    - For float types: NaN and rounding normalizations
    - For boolean: normalize
    - For object: try to parse as date/datetime if it looks like that
    """
    # Try pandas dtype detection, but tolerate mixed types
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, errors='coerce').dt.strftime("%Y-%m-%d %H:%M:%S.%f").str.rstrip('0').str.rstrip('.').fillna('')  # Standardize format, remove trailing 0s & dot if needed
    elif pd.api.types.is_float_dtype(series):
        # Some database engines will use different precision; round gently
        return series.round(6)  # Use 6 decimals for comparison
    elif pd.api.types.is_bool_dtype(series):
        return series.astype(int)
    elif pd.api.types.is_integer_dtype(series):
        return series.fillna(0)
    else: # object
        # Try to parse datetime-like objects
        try:
            out = pd.to_datetime(series, errors='coerce')
            mask = ~out.isna()
            if mask.any():
                # Some can be parsed, treat all as datetime
                return out.dt.strftime("%Y-%m-%d %H:%M:%S.%f").str.rstrip('0').str.rstrip('.').fillna(series.astype(str))
            else:
                # If nothing can be parsed as date, fall back to str
                return series.astype(str).replace({'nan':'', 'NaT':''})
        except Exception:
            return series.astype(str).replace({'nan':'', 'NaT':''})

def normalize_dataframe_types_for_comparison(df):
    """
    Return a new dataframe with columns normalized for fair cross-db comparison.
    """
    df_norm = pd.DataFrame()
    for c in df.columns:
        try:
            df_norm[c] = normalize_column(c, df[c])
        except Exception as e:
            df_norm[c] = df[c].astype(str)  # fallback: string
    return df_norm

# ---------------- Compare Logic ---------------- #

def compare_tables(table, eng1, eng2, diff_engine, progress):
    cols1 = get_columns(eng1, OLDER_DUMP_TYPE, table)
    cols2 = get_columns(eng2, NEWER_DUMP_TYPE, table)

    # Remove all extra columns from comparison
    cols1_wo_extra = [c for c in cols1 if c not in EXTRA_COLUMNS]
    cols2_wo_extra = [c for c in cols2 if c not in EXTRA_COLUMNS]

    # Find common columns (ignoring all extra columns)
    common_cols = [c for c in cols1_wo_extra if c in cols2_wo_extra]

    if not common_cols:
        log(f"⚠️ Skipping {table}: NO COMMON COLUMNS.")
        progress[table]["status"] = "no_common_columns"
        progress[table]["diff_rows"] = 0
        save_progress(progress)
        return 0

    pkey = get_primary_key(eng2, NEWER_DUMP_TYPE, table)
    log(f"Comparing: {table} (PK={pkey}, common_cols={len(common_cols)})")

    try:
        if NEWER_DUMP_TYPE == "mssql":
            col_str = ', '.join([f'[{c}]' for c in common_cols])
            sel_table = f"[{table}]"
        else:
            col_str = ', '.join([f"`{c}`" for c in common_cols])
            sel_table = f"`{table}`"
        q2 = f"SELECT {col_str} FROM {sel_table}"
        df2 = pd.read_sql(q2, eng2)

        if df2.empty:
            log(f"{table}: second dump empty; skipping.")
            return 0

        if OLDER_DUMP_TYPE == "mssql":
            col_str_old = ', '.join([f'[{c}]' for c in common_cols])
            sel_table_old = f"[{table}]"
        else:
            col_str_old = ', '.join([f"`{c}`" for c in common_cols])
            sel_table_old = f"`{table}`"
        q1 = f"SELECT {col_str_old} FROM {sel_table_old}"
        df1 = pd.read_sql(q1, eng1)

        # --- NORMALIZATION FOR CROSS-DB DIFFS ---
        df2_norm = normalize_dataframe_types_for_comparison(df2)
        df1_norm = normalize_dataframe_types_for_comparison(df1)

        # -----------------------------------------------------------------
        # Special case for float columns! Patch for @code block (128-131) type mismatch
        # If any merge fails due to float/object mismatch, fallback to tolerant compare for such columns
        try:
            merged = df2_norm.merge(df1_norm, how="left", on=common_cols, indicator=True)
        except Exception as merge_exc:
            # Try to find object/float columns in both and fallback to string for those columns for merge
            fallback_cols = []
            for col in common_cols:
                t1 = str(df1_norm[col].dtype)
                t2 = str(df2_norm[col].dtype)
                if ("float" in t1 or "object" in t1 or "float" in t2 or "object" in t2):
                    fallback_cols.append(col)
            # Apply .astype(str) to fallback columns for both dfs & merge again
            for col in fallback_cols:
                df2_norm[col] = df2_norm[col].astype(str)
                df1_norm[col] = df1_norm[col].astype(str)
            merged = df2_norm.merge(df1_norm, how="left", on=common_cols, indicator=True)
        # -----------------------------------------------------------------

        diff_idx = merged[merged["_merge"] == "left_only"].index
        # Get diff rows from non-normalized df2 (so we write real values!), but restrict to the corresponding indices
        diff = df2.loc[diff_idx]
        if diff.empty:
            log(f"✅ {table}: No diffs found.")
            return 0

        ensure_diff_table(diff_engine, table, diff.columns)
        count = insert_diff_rows(diff_engine, table, diff)
        log(f"✅ {table}: {count} diff rows inserted.")
        return count

    except Exception as e:
        log(f"❌ Error diffing {table}: {e}")
        return 0

# ---------------- Main ---------------- #

def main():
    log("🚀 Starting dump diff extractor with resume support")

    # Ensure diff MySQL database exists before proceeding
    try:
        ensure_mysql_database_exists(DIFF_DUMP_CONN_STR, DIFF_DB_NAME)
    except Exception as e:
        log(f"❌ Could not create or check diff database: {e}")
        return

    # Now create engine for the DIFF database after it's been created above
    if OLDER_DUMP_TYPE == "mssql":
        eng1 = mssql_engine(OLDER_DUMP_CONN_STR)
    else:
        eng1 = mysql_engine(OLDER_DUMP_CONN_STR)

    if NEWER_DUMP_TYPE == "mssql":
        eng2 = mssql_engine(NEWER_DUMP_CONN_STR)
    else:
        eng2 = mysql_engine(NEWER_DUMP_CONN_STR)

    try:
        diff_engine = mysql_engine(DIFF_DUMP_CONN_STR)
    except Exception as e:
        log(f"❌ Could not connect to newly ensured diff database: {e}")
        return

    all_tables = get_table_list(eng2, NEWER_DUMP_TYPE)
    log(f"Tables detected: {len(all_tables)}")

    progress = load_progress()

    # Initialize missing entries
    for t in all_tables:
        if t not in progress or progress[t].get('status') not in ["done", "no_common_columns"]:
            progress[t] = {"status": "pending", "diff_rows": 0}

    save_progress(progress)

    summary = []

    for table in tqdm(all_tables, desc="Tables", unit="tbl"):
        status = progress[table]["status"]

        if status in ("done", "error", "no_common_columns"):
            log(f"⏭ Skipping {table}: already marked as {status}.")
            continue

        try:
            n = compare_tables(table, eng1, eng2, diff_engine, progress)

            if progress[table]["status"] != "no_common_columns":
                progress[table]["status"] = "done"
                progress[table]["diff_rows"] = n

            save_progress(progress)
            summary.append((table, n))

        except Exception as e:
            log(f"❌ Exception in {table}: {e}")
            progress[table]["status"] = "error"
            progress[table]["error"] = str(e)
            save_progress(progress)
            summary.append((table, "ERROR"))

    total = sum((x[1] if isinstance(x[1], int) else 0) for x in summary)
    log("=" * 60)
    log(f"All done! Inserted {total} diff rows.")

if __name__ == "__main__":
    main()
