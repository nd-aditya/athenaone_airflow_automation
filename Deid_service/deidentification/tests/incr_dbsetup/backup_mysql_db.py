import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# CONFIG
# -----------------------------
# Set your source and target MySQL connection strings
SOURCE_MYSQL_CONN = "mysql+pymysql://root:123456789@localhost:3306/automation_incr_testing_backup"
TARGET_MYSQL_CONN = "mysql+pymysql://root:123456789@localhost:3306/automation_incr_testing"

CHUNK_SIZE = 50000   # adjust based on memory
COPY_MODE = "append"  # use "append" or "replace"

# -----------------------------
# CONNECT ENGINES
# -----------------------------
source_mysql_engine = create_engine(SOURCE_MYSQL_CONN)
target_mysql_engine = create_engine(TARGET_MYSQL_CONN)

# -----------------------------
# FETCH LIST OF TABLES
# -----------------------------
def get_mysql_tables():
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
    """
    df = pd.read_sql(query, source_mysql_engine)
    # Some MySQL installations may return the column as 'TABLE_NAME' (upper case), especially with certain configuration/collation settings.
    # Fallback to upper-case if lower-case is not present.
    col_name = None
    for n in df.columns:
        if n.lower() == "table_name":
            col_name = n
            break
    if col_name is None:
        raise KeyError(f"Could not find 'table_name' column in MySQL information_schema result (columns: {df.columns.tolist()})")
    return [t for t in df[col_name].tolist()]

# -----------------------------
# COPY SINGLE TABLE
# -----------------------------
def copy_table(table_name):
    print(f"\n📌 Copying table: {table_name}")

    try:
        # read in chunks
        offset = 0
        chunk_number = 1

        while True:
            query = f"SELECT * FROM {table_name} LIMIT {CHUNK_SIZE} OFFSET {offset}"
            df = pd.read_sql(query, source_mysql_engine)

            if df.empty:
                print(f"✔ Completed table: {table_name}")
                break

            print(f"  → Loading chunk {chunk_number} ({len(df)} rows)...")

            # Write to target MySQL
            df.to_sql(
                table_name,
                target_mysql_engine,
                if_exists=COPY_MODE,
                index=False,
                chunksize=CHUNK_SIZE
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
    tables = get_mysql_tables()
    print(f"Found {len(tables)} tables.\n")

    for table in tables:
        copy_table(table)

    print("\n🎉 MySQL-to-MySQL Copy Completed Successfully!")

if __name__ == "__main__":
    main()
