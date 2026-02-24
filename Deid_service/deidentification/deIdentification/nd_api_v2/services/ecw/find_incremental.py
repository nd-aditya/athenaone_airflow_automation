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
from urllib.parse import quote_plus

from datetime import datetime, date
import pandas as pd
from tqdm import tqdm
import traceback
from sqlalchemy.engine.url import make_url
from sqlalchemy import create_engine, text
from .utils import (
    load_find_incremental_logs,
    save_find_incremental_logs,
    get_incremental_diff_database_name,
    EXTRA_COLUMNS
)
import time

# Try to import pyodbc for ODBC connections
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    print("Warning: pyodbc not available. ODBC connections will not work.")

def get_newer_database_name(config: dict):
    if "database" in config['newer_database_details']:
        return config['newer_database_details']['database']
    client_name = config['client_name']
    dump_date = config['dump_date'].replace("-", "")
    return f"{client_name}_{dump_date}"

def get_older_dump_connection_string(config: dict):
    main_database_details = config['main_database_details']
    dump_type = main_database_details['db_type']
    if dump_type == "mssql":
        return f"mssql+pymssql://{main_database_details['username']}:{main_database_details['password']}@{main_database_details['host']}:{main_database_details['port']}/{main_database_details['database_name']}"
    else:
        return f"mysql+pymysql://{main_database_details['username']}:{main_database_details['password']}@{main_database_details['host']}:{main_database_details['port']}/{main_database_details['database_name']}"

def older_dump_engine(config: dict):
    return create_engine(get_older_dump_connection_string(config))

def make_mssql_odbc_connection_string(db_details, database_name=None):
    """Create ODBC connection string for MSSQL with ODBC driver"""
    parts = [
        f"Driver={{{db_details.get('driver', 'ODBC Driver 17 for SQL Server')}}}",
        f"Server={db_details['host']},{db_details['port']}",
        f"Uid={db_details['username']}",
        f"Pwd={db_details['password']}",
        f"Encrypt={db_details.get('encrypt', 'no')}",
        f"TrustServerCertificate={db_details.get('trustservercertificate', 'yes')}"
    ]
    # Only add Database if specified
    if database_name:
        parts.insert(2, f"Database={database_name}")  # Insert after Server
    return ";".join(parts)

def newer_dump_engine(config: dict):
    d = config['newer_database_details']

    db_type = d.get('db_type')
    user = quote_plus(d['username'])
    password = quote_plus(d['password'])
    host = d['host']
    port = d['port']
    database = d['database']

    if db_type == "mssql":
        return create_engine(
            f"mssql+pymssql://{user}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            pool_recycle=3600
        )

    elif db_type == "mysql":
        return create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True,
            pool_recycle=3600,
            charset="utf8mb4"
        )

    else:
        raise ValueError(f"Unsupported db_type: {db_type}")

def get_diff_database_connection_string(config: dict):
    diff_database_name = get_incremental_diff_database_name(config)
    incr_diff_db_details = config['incremental_diff_database_details']
    return f"mysql+mysqlconnector://{incr_diff_db_details['username']}:{incr_diff_db_details['password']}@{incr_diff_db_details['host']}:{incr_diff_db_details['port']}/{diff_database_name}"

def get_diff_engine(config: dict):
    return create_engine(get_diff_database_connection_string(config))

# --------------- Temporary Local Database Functions ---------------

def get_temporary_database_name(config: dict):
    """Get the name for the temporary local database"""
    client_name = config['client_name']
    dump_date = config['dump_date'].replace("-", "")
    return f"{client_name}_dump_date_{dump_date}_temporary"

def get_temporary_database_connection_string(config: dict):
    """Get connection string for temporary database (uses incremental_diff_database_details server)"""
    temp_db_name = get_temporary_database_name(config)
    incr_diff_db_details = config['temporary_database_details']
    return f"mysql+mysqlconnector://{incr_diff_db_details['username']}:{incr_diff_db_details['password']}@{incr_diff_db_details['host']}:{incr_diff_db_details['port']}/{temp_db_name}"

def get_temporary_engine(config: dict):
    """Get engine for temporary local database"""
    return create_engine(get_temporary_database_connection_string(config))

def ensure_temporary_database_exists(config: dict, drop_if_exists=True):
    """Create temporary database if it doesn't exist"""
    temp_db_name = get_temporary_database_name(config)
    incr_diff_db_details = config['temporary_database_details']
    conn_str = f"mysql+mysqlconnector://{incr_diff_db_details['username']}:{incr_diff_db_details['password']}@{incr_diff_db_details['host']}:{incr_diff_db_details['port']}"
    
    return ensure_mysql_database_exists(conn_str, temp_db_name, drop_if_exists=drop_if_exists)

def drop_temporary_database(config: dict):
    """Drop the temporary database and all its tables"""
    temp_db_name = get_temporary_database_name(config)
    incr_diff_db_details = config['temporary_database_details']
    
    # Parse URL
    conn_str = f"mysql+mysqlconnector://{incr_diff_db_details['username']}:{incr_diff_db_details['password']}@{incr_diff_db_details['host']}:{incr_diff_db_details['port']}"
    url_obj = make_url(conn_str)
    
    # Remove any existing database part for server-level connection
    url_no_db = url_obj.set(database=None)
    
    # Connect to MySQL server (not any DB)
    engine = create_engine(url_no_db, pool_pre_ping=True)
    
    try:
        with engine.connect() as conn:
            # Check database existence
            res = conn.execute(
                text("""
                    SELECT SCHEMA_NAME
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME = :db
                """),
                {"db": temp_db_name}
            ).fetchone()
            
            if res is not None:
                print(f"Dropping temporary database '{temp_db_name}'...")
                conn.execute(text(f"DROP DATABASE IF EXISTS `{temp_db_name}`"))
                conn.commit()
                print(f"Temporary database '{temp_db_name}' dropped successfully.")
                return True
            else:
                print(f"Temporary database '{temp_db_name}' does not exist, nothing to clean up.")
                return False
    except Exception as e:
        print(f"Warning: Could not drop temporary database '{temp_db_name}': {e}")
        traceback.print_exc()
        return False
    finally:
        engine.dispose()

def copy_table_to_temporary(source_engine, target_engine, table_name, progress, config, chunk_size=50000):
    """Copy a table from source to target database in chunks with progress tracking"""
    print(f"Copying table {table_name} to temporary database...")
    
    try:
        # Initialize copy progress tracking for this table if not exists
        if 'copy_status' not in progress.get(table_name, {}):
            progress[table_name] = progress.get(table_name, {})
            progress[table_name]['copy_status'] = 'pending'
            progress[table_name]['copy_offset'] = 0
            progress[table_name]['copy_chunk_number'] = 1
            progress[table_name]['copy_total_rows'] = 0
        
        copy_progress = progress[table_name]
        
        # Check if copy is already complete
        if copy_progress.get('copy_status') == 'done':
            print(f"Skipping copy for {table_name}: already completed ({copy_progress.get('copy_total_rows', 0)} rows)")
            return True
        
        # Resume from saved progress if copy was interrupted
        offset = copy_progress.get('copy_offset', 0)
        chunk_number = copy_progress.get('copy_chunk_number', 1)
        total_rows = copy_progress.get('copy_total_rows', 0)
        
        # Check if table exists in target (for resume scenario)
        table_exists_in_target = table_exists(target_engine, table_name)
        table_created = table_exists_in_target
        
        # Determine if we're resuming or starting fresh
        is_resuming = (offset > 0 or table_exists_in_target) and copy_progress.get('copy_status') == 'in_progress'
        
        if is_resuming:
            if table_exists_in_target:
                print(f"Resuming copy for {table_name} from offset {offset} (chunk {chunk_number}, {total_rows} rows already copied)")
            else:
                # Table doesn't exist but we have offset > 0, reset to start
                print(f"Table {table_name} not found in target but offset > 0. Resetting to start.")
                offset = 0
                chunk_number = 1
                total_rows = 0
                table_created = False
                is_resuming = False
        
        # Get or load metadata (columns, primary key, etc.)
        if is_resuming and 'source_cols' in copy_progress:
            # Use saved metadata when resuming
            source_cols = copy_progress['source_cols']
            pkey = copy_progress.get('pkey')
            col_str = copy_progress['col_str']
            sel_table = copy_progress['sel_table']
        else:
            # Get columns from source (first run or metadata missing)
            source_cols = get_columns(source_engine, table_name)
            if not source_cols:
                print(f"No columns found for {table_name}, skipping")
                copy_progress['copy_status'] = 'error'
                copy_progress['copy_error'] = 'No columns found'
                save_find_incremental_logs(progress, config)
                return False
            
            # Get primary key for ordering (helps with MSSQL OFFSET/FETCH)
            pkey = get_primary_key(source_engine, table_name)
            
            # Build column string based on database type
            if source_engine.dialect.name == "mssql":
                col_str = ', '.join([f'[{c}]' for c in source_cols])
                sel_table = f"[{table_name}]"
            else:
                col_str = ', '.join([f"`{c}`" for c in source_cols])
                sel_table = f"`{table_name}`"
            
            # Drop table if exists in target (only on first run, not when resuming)
            if not is_resuming:
                with target_engine.connect() as conn:
                    if target_engine.dialect.name == "mssql":
                        conn.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]"))
                    else:
                        conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
                    conn.commit()
            
            # Store metadata for resuming
            copy_progress['source_cols'] = source_cols
            copy_progress['pkey'] = pkey
            copy_progress['col_str'] = col_str
            copy_progress['sel_table'] = sel_table
            copy_progress['copy_status'] = 'in_progress'
            save_find_incremental_logs(progress, config)
        
        while True:
            # Build query with offset/limit based on database type
            if source_engine.dialect.name == "mssql":
                # MSSQL requires ORDER BY for OFFSET/FETCH
                if pkey:
                    order_by = f"ORDER BY [{pkey}]"
                else:
                    # Use all columns for ordering if no primary key (less efficient but works)
                    order_cols = ', '.join([f'[{c}]' for c in source_cols[:5]])  # Use first 5 columns
                    order_by = f"ORDER BY {order_cols}"
                query = f"SELECT {col_str} FROM {sel_table} {order_by} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
            else:
                # MySQL uses LIMIT/OFFSET
                query = f"SELECT {col_str} FROM {sel_table} LIMIT {chunk_size} OFFSET {offset}"
            
            df_chunk = pd.read_sql(query, source_engine)
            
            # Create table on first chunk using pandas to_sql (preserves data types better)
            if not table_created:
                df_chunk.to_sql(table_name, target_engine, if_exists="replace", index=False, method="multi", chunksize=chunk_size)
                table_created = True
            else:
                # Append subsequent chunks
                df_chunk.to_sql(table_name, target_engine, if_exists="append", index=False, method="multi", chunksize=chunk_size)
            
            if df_chunk.empty:
                break
            
            total_rows += len(df_chunk)
            print(f"  → Copied chunk {chunk_number} ({len(df_chunk)} rows, total: {total_rows})")
            
            # Update progress after each chunk
            copy_progress['copy_offset'] = offset + chunk_size
            copy_progress['copy_chunk_number'] = chunk_number + 1
            copy_progress['copy_total_rows'] = total_rows
            copy_progress['copy_status'] = 'in_progress'
            save_find_incremental_logs(progress, config)
            
            offset += chunk_size
            chunk_number += 1
        
        # Mark copy as complete
        copy_progress['copy_status'] = 'done'
        copy_progress['copy_offset'] = offset  # Final offset
        copy_progress['copy_chunk_number'] = chunk_number
        copy_progress['copy_total_rows'] = total_rows
        save_find_incremental_logs(progress, config)
        
        print(f"Successfully copied {table_name}: {total_rows} rows")
        return True
        
    except Exception as e:
        print(f"Error copying table {table_name}: {e}")
        traceback.print_exc()
        # Mark copy as error
        if table_name in progress:
            progress[table_name]['copy_status'] = 'error'
            progress[table_name]['copy_error'] = str(e)
            save_find_incremental_logs(progress, config)
        return False

def fetch_all_tables_to_local(config: dict, newer_engine, temporary_engine, all_tables, progress, chunk_size=50000):
    """Fetch all tables from newer database to temporary local database"""
    print("Starting to fetch tables to temporary local database...")
    print(f"Total tables to copy: {len(all_tables)}")
    
    for table in tqdm(all_tables, desc="Copying tables", unit="tbl"):
        copy_table_to_temporary(newer_engine, temporary_engine, table, progress, config, chunk_size)
    
    print("All tables copied to temporary database")

def drop_table_from_temporary(temporary_engine, table_name):
    """Drop a single table from the temporary database"""
    try:
        with temporary_engine.connect() as conn:
            if temporary_engine.dialect.name == "mssql":
                conn.execute(text(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]"))
            else:
                conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
            conn.commit()
        return True
    except Exception as e:
        print(f"Warning: Could not drop table '{table_name}' from temporary database: {e}")
        return False


# --------------- Create MySQL Database if Not Exists ---------------

def ensure_mysql_database_exists(conn_str, db_name, drop_if_exists=True):
    # Parse URL
    url_obj = make_url(conn_str)

    # Remove any existing database part for server-level connection
    url_no_db = url_obj.set(database=None)

    # Connect to MySQL server (not any DB)
    engine = create_engine(url_no_db, pool_pre_ping=True)
    with engine.connect() as conn:
        # Check database existence
        res = conn.execute(
            text("""
                SELECT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME = :db
            """),
            {"db": db_name}
        ).fetchone()

        if res is not None and drop_if_exists:
            print(f"Database '{db_name}' already exists. Dropping it first...")
            conn.execute(text(f"DROP DATABASE IF EXISTS `{db_name}`"))
            print(f"Database '{db_name}' dropped.")

        print(f"Creating database '{db_name}'...")
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8mb4"))
        print(f"Database '{db_name}' created successfully.")

    return True
# ---------------- Database Helpers ---------------- #
def mssql_engine(conn_str):
    try:
        eng = create_engine(conn_str, pool_pre_ping=True)
        with eng.connect():
            pass
        return eng
    except Exception as e:
        print(f"Error connecting to MSSQL: {e}")
        raise

def get_table_list(engine):
    # Note: The type must be EXACTLY "mssql" or "mysql"
    db_type = engine.dialect.name
    if db_type == "mssql":
        q = text("SELECT name FROM sys.tables WHERE is_ms_shipped=0 ORDER BY name")
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
            return [r[0] for r in rows]
    else:
        df = pd.read_sql(text("SHOW TABLES"), engine)
        return df.iloc[:, 0].tolist()

def get_columns(engine, table):
    db_type = engine.dialect.name
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

def get_primary_key(engine, table):
    db_type = engine.dialect.name
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

def table_exists(engine, table_name):
    """Check if a table exists in the database"""
    db_type = engine.dialect.name
    try:
        with engine.connect() as conn:
            if db_type == "mssql":
                q = text("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :table")
                result = conn.execute(q, {"table": table_name}).fetchone()
                return result[0] > 0 if result else False
            else:
                q = text("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table")
                result = conn.execute(q, {"table": table_name}).fetchone()
                return result[0] > 0 if result else False
    except Exception:
        return False

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
        print(f"Failed to insert diff rows for {table}: {e}")
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
def compare_tables(table: str, eng1, eng2, diff_engine, progress: dict, config: dict, chunk_size=50000) -> int:
    cols1 = get_columns(eng1, table)
    cols2 = get_columns(eng2, table)

    # Remove all extra columns from comparison
    cols1_wo_extra = [c for c in cols1 if c not in EXTRA_COLUMNS]
    cols2_wo_extra = [c for c in cols2 if c not in EXTRA_COLUMNS]

    # Find common columns (ignoring all extra columns)
    common_cols = [c for c in cols1_wo_extra if c in cols2_wo_extra]

    if not common_cols:
        print(f"Skipping {table}: NO COMMON COLUMNS.")
        progress[table]["diff_status"] = "no_common_columns"
        progress[table]["diff_rows"] = 0
        save_find_incremental_logs(progress, config)
        return 0

    pkey = get_primary_key(eng2, table)
    print(f"Comparing: {table} (PK={pkey}, common_cols={len(common_cols)})")

    try:
        # Load historical data (older dump) once - this is typically smaller
        if eng1.dialect.name == "mssql":
            col_str_old = ', '.join([f'[{c}]' for c in common_cols])
            sel_table_old = f"[{table}]"
        else:
            col_str_old = ', '.join([f"`{c}`" for c in common_cols])
            sel_table_old = f"`{table}`"
        q1 = f"SELECT {col_str_old} FROM {sel_table_old}"
        df1 = pd.read_sql(q1, eng1)
        
        if df1.empty:
            print(f"{table}: historical dump empty; treating all newer rows as diffs.")
            df1 = pd.DataFrame(columns=common_cols)

        # --- NORMALIZATION FOR CROSS-DB DIFFS ---
        df1_norm = normalize_dataframe_types_for_comparison(df1)

        # Now read newer database in chunks and compare
        if eng2.dialect.name == "mssql":
            col_str = ', '.join([f'[{c}]' for c in common_cols])
            sel_table = f"[{table}]"
        else:
            col_str = ', '.join([f"`{c}`" for c in common_cols])
            sel_table = f"`{table}`"

        # Get primary key for ordering (helps with MSSQL OFFSET/FETCH)
        pkey = get_primary_key(eng2, table)

        # Process in chunks
        offset = 0
        chunk_number = 1
        total_diff_rows = 0
        table_created = False
        
        while True:
            # Build query with offset/limit based on database type
            if eng2.dialect.name == "mssql":
                # MSSQL requires ORDER BY for OFFSET/FETCH
                if pkey and pkey in common_cols:
                    order_by = f"ORDER BY [{pkey}]"
                elif pkey:
                    # If PK exists but not in common_cols, use it anyway for ordering
                    order_by = f"ORDER BY [{pkey}]"
                else:
                    # Use first few common columns for ordering if no primary key
                    order_cols = ', '.join([f'[{c}]' for c in common_cols[:5]])
                    order_by = f"ORDER BY {order_cols}"
                q2 = f"SELECT {col_str} FROM {sel_table} {order_by} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
            else:
                # MySQL uses LIMIT/OFFSET
                q2 = f"SELECT {col_str} FROM {sel_table} LIMIT {chunk_size} OFFSET {offset}"
            
            df2_chunk = pd.read_sql(q2, eng2)
            
            if df2_chunk.empty:
                break

            # Normalize chunk
            df2_chunk_norm = normalize_dataframe_types_for_comparison(df2_chunk)

            # -----------------------------------------------------------------
            # Special case for float columns! Patch for @code block (128-131) type mismatch
            # If any merge fails due to float/object mismatch, fallback to tolerant compare for such columns
            try:
                merged = df2_chunk_norm.merge(df1_norm, how="left", on=common_cols, indicator=True)
            except Exception as merge_exc:
                # Try to find object/float columns in both and fallback to string for those columns for merge
                fallback_cols = []
                for col in common_cols:
                    t1 = str(df1_norm[col].dtype)
                    t2 = str(df2_chunk_norm[col].dtype)
                    if ("float" in t1 or "object" in t1 or "float" in t2 or "object" in t2):
                        fallback_cols.append(col)
                # Apply .astype(str) to fallback columns for both dfs & merge again
                for col in fallback_cols:
                    df2_chunk_norm[col] = df2_chunk_norm[col].astype(str)
                    df1_norm[col] = df1_norm[col].astype(str)
                merged = df2_chunk_norm.merge(df1_norm, how="left", on=common_cols, indicator=True)
            # -----------------------------------------------------------------

            diff_idx = merged[merged["_merge"] == "left_only"].index

            # Get diff rows from non-normalized df2_chunk (so we write real values!)
            diff = df2_chunk.loc[diff_idx] if len(diff_idx) > 0 else pd.DataFrame(columns=df1_norm.columns)
            
            # if not diff.empty:
            if not table_created:
                ensure_diff_table(diff_engine, table, diff.columns)
                table_created = True
            
            count = insert_diff_rows(diff_engine, table, diff)
            total_diff_rows += count
            print(f"  → Chunk {chunk_number}: {count} diff rows found (total: {total_diff_rows})")
            
            offset += chunk_size
            chunk_number += 1

        if total_diff_rows == 0:
            print(f"{table}: No diffs found.")
        else:
            print(f"{table}: {total_diff_rows} diff rows inserted.")
        
        return total_diff_rows

    except Exception as e:
        print(f"Error diffing {table}: {e}")
        traceback.print_exc()
        return 0


def find_incremental_diff(config: dict, retriggerd=False):
    print("Starting dump diff extractor with resume support")
    retriggerd = config.get("retriggerd", False)
    
    # Check if we should fetch tables to local temporary database
    fetch_to_local = config.get('fetch_table_to_local', False)
    chunk_size = config.get('extraction_settings', {}).get('chunk_size', 50000)
    
    try:
        drop_if_exists = False if retriggerd else True
        ensure_mysql_database_exists(get_older_dump_connection_string(config), get_incremental_diff_database_name(config), drop_if_exists=drop_if_exists)
    except Exception as e:
        print(traceback.format_exc())
        print(f"Could not create or check diff database: {e}")
        return

    # Now create engine for the DIFF database after it's been created above
    eng1 = older_dump_engine(config)
    eng2 = newer_dump_engine(config)
    diff_engine = get_diff_engine(config)

    all_tables = get_table_list(eng2)
    print(f"Tables detected: {len(all_tables)}")

    # If fetch_to_local is enabled, create temporary database
    temporary_engine = None
    original_eng2 = eng2  # Keep reference to original newer engine
    
    if fetch_to_local:
        print("=" * 60)
        print("Fetch to local enabled: Will copy tables one at a time")
        print("=" * 60)
        
        try:
            # Create temporary database
            drop_if_exists = False if retriggerd else True
            # ensure_temporary_database_exists(config, drop_if_exists=drop_if_exists)
            temporary_engine = get_temporary_engine(config)
            print("Temporary database created")
        except Exception as e:
            print(f"Warning: Failed to create temporary database: {e}")
            print("Falling back to direct connection to newer database...")
            traceback.print_exc()
            temporary_engine = None

    progress = load_find_incremental_logs(config)

    # Initialize missing entries (support legacy 'status' key for backward compatibility)
    for t in all_tables:
        existing_entry = progress.get(t, {})
        legacy_status = existing_entry.get("diff_status")
        if t not in progress or legacy_status not in ["done", "no_common_columns"]:
            progress[t] = {
                "diff_status": legacy_status if legacy_status else "pending",
                "diff_rows": existing_entry.get("diff_rows", 0),
            }

    save_find_incremental_logs(progress, config)

    summary = []

    for table in tqdm(all_tables, desc="Tables", unit="tbl"):
        status = progress[table].get("diff_status") or progress[table].get("status")

        if status in ("done", "error", "no_common_columns"):
            print(f"⏭ Skipping {table}: already marked as {status}.")
            continue

        # If fetch_to_local is enabled, copy this table to temporary database first
        if fetch_to_local and temporary_engine is not None:
            print(f"\n{'='*60}")
            print(f"Step 1: Copying table '{table}' to temporary database...")
            print(f"{'='*60}")
            try:
                copy_table_to_temporary(original_eng2, temporary_engine, table, progress, config, chunk_size)
                # Use temporary engine for comparison
                eng2 = temporary_engine
                print(f"Table '{table}' copied. Using temporary database for comparison.")
            except Exception as e:
                print(f"Warning: Failed to copy table '{table}' to temporary database: {e}")
                print("Falling back to direct connection to newer database for this table...")
                traceback.print_exc()
                eng2 = original_eng2

        try:
            print(f"\n{'='*60}")
            print(f"Step 2: Finding differences for table '{table}'...")
            print(f"{'='*60}")
            n = compare_tables(table, eng1, eng2, diff_engine, progress, config, chunk_size)

            if progress[table]["diff_status"] != "no_common_columns":
                progress[table]["diff_status"] = "done"
                progress[table]["diff_rows"] = n

            save_find_incremental_logs(progress, config)
            summary.append((table, n))

            # Clean up: Drop the table from temporary database after comparison
            if fetch_to_local and temporary_engine is not None:
                print(f"\n{'='*60}")
                print(f"🧹 Step 3: Cleaning up table '{table}' from temporary database...")
                print(f"{'='*60}")
                # drop_table_from_temporary(temporary_engine, table)
                print(f"Table '{table}' removed from temporary database.")

        except Exception as e:
            print(f"Exception in {table}: {e}")
            progress[table]["diff_status"] = "error"
            progress[table]["error"] = str(e)
            save_find_incremental_logs(progress, config)
            summary.append((table, "ERROR"))
            
            
    total = sum((x[1] if isinstance(x[1], int) else 0) for x in summary)
    print("=" * 60)
    print(f"All done! Inserted {total} diff rows.")
    
    # Clean up temporary database if it was created
    if fetch_to_local and temporary_engine is not None:
        print("=" * 60)
        print("🧹 Cleaning up temporary database...")
        print("=" * 60)
        try:
            # Close the temporary engine connection first
            temporary_engine.dispose()
            # Drop the temporary database
            # drop_temporary_database(config)
        except Exception as e:
            print(f"Warning: Error during cleanup: {e}")
            traceback.print_exc()
    
    return total