import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
import decimal

# -----------------------------
# CONFIG
# -----------------------------
MYSQL_CONN = "mysql+pymysql://root:123456789@localhost:3306/automation_incr_testing"
MSSQL_CONN = "mssql+pymssql://sa:ndAdmin2025@localhost:1433/automation_incr_testing"

CHUNK_SIZE = 50000   # adjust based on memory
COPY_MODE = "append"  # use "append" or "replace"

# -----------------------------
# CONNECT ENGINES
# -----------------------------
mysql_engine = create_engine(MYSQL_CONN)
mssql_engine = create_engine(MSSQL_CONN)

# -----------------------------
# FETCH LIST OF TABLES (now from MSSQL)
# -----------------------------
def get_mssql_tables():
    query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
    """
    df = pd.read_sql(query, mssql_engine)
    col_name = None
    for n in df.columns:
        if n.lower() == "table_name":
            col_name = n
            break
    if col_name is None:
        raise KeyError(f"Could not find 'TABLE_NAME' column in MSSQL information_schema result (columns: {df.columns.tolist()})")
    return [t for t in df[col_name].tolist()]

# -----------------------------
# UTILS: Get column types from MSSQL
# -----------------------------
def get_column_info(engine, table_name):
    insp = inspect(engine)
    col_info = {}
    for col in insp.get_columns(table_name):
        col_name = col['name']
        type_str = str(col['type']).lower()
        col_info[col_name] = type_str
    return col_info

def is_datetime_type(type_str):
    return any(
        substr in type_str
        for substr in ["datetime", "timestamp"]
    )

def is_time_type(type_str):
    return "time" in type_str and "datetime" not in type_str

def is_date_type(type_str):
    return type_str == "date"

def is_decimal_type(type_str):
    return any(substr in type_str for substr in ["decimal", "numeric"])

def is_float_type(type_str):
    return any(substr in type_str for substr in ["float", "double"])

# -----------------------------
# COPY SINGLE TABLE from MSSQL to MySQL, preserving exact values
# -----------------------------
def copy_table(table_name):
    print(f"\n📌 Copying table: {table_name}")
    try:
        # Get full type info
        src_col_types = get_column_info(mssql_engine, table_name)
        offset = 0
        chunk_number = 1

        while True:
            query = f"SELECT * FROM {table_name} ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {CHUNK_SIZE} ROWS ONLY"
            df = pd.read_sql(query, mssql_engine)

            if df.empty:
                print(f"✔ Completed table: {table_name}")
                break

            # Careful type preservation (applies to each chunk, does NOT modify in db)
            for col, type_str in src_col_types.items():
                if col not in df.columns:
                    continue
                if is_datetime_type(type_str):
                    # Keep microseconds (SQL Server max 7 decimals, MySQL DATETIME(6) supports up to 6)
                    # Use string with all subsecond info; MySQL DATETIME(6) will accept.
                    def fmt_dt(x):
                        if pd.isnull(x):
                            return None
                        # Safely format with max MS SQL precision (microseconds/truncate if needed)
                        val = str(x)
                        # The Pandas datetime will have up to nanosecond, but PyMySQL/MySQL ignore >6 decimals
                        # So, we reformat to what SQL Server string would look like
                        # e.g. 2024-01-02 01:02:03.123456 or .123
                        if "." in val:
                            # MSSQL sometimes returns 3 decimals only, keep as-is
                            parts = val.split(".")
                            return parts[0] + "." + parts[1][:6]
                        else:
                            return val
                    df[col] = df[col].apply(fmt_dt)
                elif is_time_type(type_str):
                    def fmt_time(x):
                        if pd.isnull(x):
                            return None
                        val = str(x)
                        if "." in val:
                            parts = val.split(".")
                            return parts[0] + "." + parts[1][:6]
                        else:
                            return val
                    df[col] = df[col].apply(fmt_time)
                elif is_date_type(type_str):
                    # Convert to str to ensure MySQL not parse/round
                    def fmt_date(x):
                        if pd.isnull(x):
                            return None
                        return x.strftime("%Y-%m-%d") if hasattr(x, 'strftime') else str(x)
                    df[col] = df[col].apply(fmt_date)
                elif is_decimal_type(type_str):
                    # Decimal values: force to str to prevent float rounding
                    df[col] = df[col].apply(lambda x: str(x) if (not pd.isnull(x) and isinstance(x, (decimal.Decimal, float, np.number))) else x)
                elif is_float_type(type_str):
                    # Floats: try to limit loss, but aware that precision loss may occur
                    # Use repr() for all floats, so like '1.23456789012345'
                    df[col] = df[col].apply(lambda x: repr(x) if isinstance(x, float) and not pd.isnull(x) else x)
                # else: leave other types (int, str, etc.) as-is

            print(f"  → Loading chunk {chunk_number} ({len(df)} rows) into MySQL...")

            # To improve ingest fidelity, set dtype to object (avoids pandas guessing + casting)
            df = df.astype({col: "object" for col in df.columns})

            df.to_sql(
                table_name,
                mysql_engine,
                if_exists=COPY_MODE,
                index=False,
                chunksize=CHUNK_SIZE,
                method="multi"
            )

            offset += CHUNK_SIZE
            chunk_number += 1

    except SQLAlchemyError as e:
        print(f"❌ SQLAlchemy error on table {table_name}: {e}")
    except Exception as e:
        print(f"❌ Error on table {table_name}: {e}")

# -----------------------------
# COPY ALL TABLES
# -----------------------------
def main():
    tables = get_mssql_tables()
    print(f"Found {len(tables)} tables in MSSQL.\n")

    for table in tables:
        copy_table(table)

    print("\n🎉 MSSQL ➔ MySQL Migration Completed Successfully!")

if __name__ == "__main__":
    main()