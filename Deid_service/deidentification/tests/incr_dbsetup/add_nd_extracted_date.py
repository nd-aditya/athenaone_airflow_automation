import sys
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Config
CONN_STR = "mysql+mysqlconnector://root:123456789@localhost:3306/automation_incr_testing"  # Change as needed
DB_TYPE = "mysql"  # 'mysql' or 'mssql'
EXTRACTED_DATE_COL = "nd_extracted_date"
EXTRACTION_DATE = "2025-10-28"

engine = create_engine(CONN_STR)

def get_table_list(engine, db_type):
    if db_type == "mssql":
        q = text("SELECT name FROM sys.tables WHERE is_ms_shipped=0 ORDER BY name")
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
            return [r[0] for r in rows]
    else:
        df = pd.read_sql(text("SHOW TABLES"), engine)
        return df.iloc[:, 0].tolist()

def is_column_nullable_mysql(conn, table, col):
    check_nullable_sql = text("""
        SELECT IS_NULLABLE
        FROM information_schema.columns
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table AND COLUMN_NAME=:col
    """)
    rs = conn.execute(check_nullable_sql, {"table": table, "col": col})
    row = rs.fetchone()
    if row:
        # row is a SQLAlchemy Row object; access by index or key
        return str(row[0]).upper() == "YES"
    return False

def is_column_nullable_mssql(conn, table, col):
    check_nullable_sql = text("""
        SELECT IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table AND COLUMN_NAME = :col
    """)
    rs = conn.execute(check_nullable_sql, {"table": table, "col": col})
    row = rs.fetchone()
    if row:
        return str(row[0]).upper() == "YES"
    return False

def ensure_nd_extracted_date_column_or_update(engine, table, db_type):
    insp = inspect(engine)
    cols = [c['name'] if isinstance(c, dict) else c['name'] for c in insp.get_columns(table)]

    if EXTRACTED_DATE_COL not in cols:
        # Add column (as nullable first to avoid failing on existing data)
        print(f"  - Adding '{EXTRACTED_DATE_COL}' (DATE) to '{table}'...")
        if db_type == "mysql":
            alter = f'ALTER TABLE `{table}` ADD COLUMN `{EXTRACTED_DATE_COL}` DATE NULL'
            with engine.begin() as conn:
                conn.execute(text(alter))
        else:
            alter = f"ALTER TABLE [{table}] ADD [{EXTRACTED_DATE_COL}] DATE NULL"
            with engine.begin() as conn:
                conn.execute(text(alter))
    else:
        print(f"  - Table '{table}': {EXTRACTED_DATE_COL} column already present. Updating extracted date...")

    # Always update the extracted date column value after adding or if already present
    print(f"  - Setting '{EXTRACTED_DATE_COL}' = {EXTRACTION_DATE} for all rows in {table} ...")
    if db_type == "mysql":
        with engine.begin() as conn:
            conn.execute(
                text(f"UPDATE `{table}` SET `{EXTRACTED_DATE_COL}` = :extraction_date"),
                {"extraction_date": EXTRACTION_DATE},
            )
            # After populating, check if column is still nullable and make NOT NULL if needed
            try:
                if is_column_nullable_mysql(conn, table, EXTRACTED_DATE_COL):
                    print(f"  - Altering '{EXTRACTED_DATE_COL}' to NOT NULL in {table} ...")
                    conn.execute(
                        text(f"ALTER TABLE `{table}` MODIFY COLUMN `{EXTRACTED_DATE_COL}` DATE NOT NULL")
                    )
            except Exception as e:
                print(f"    !! Error during nullable check/alter for {table}: {e}")
    else:
        with engine.begin() as conn:
            conn.execute(
                text(f"UPDATE [{table}] SET [{EXTRACTED_DATE_COL}] = :extraction_date"),
                {"extraction_date": EXTRACTION_DATE},
            )
            try:
                if is_column_nullable_mssql(conn, table, EXTRACTED_DATE_COL):
                    print(f"  - Altering '{EXTRACTED_DATE_COL}' to NOT NULL in {table} ...")
                    conn.execute(
                        text(f"ALTER TABLE [{table}] ALTER COLUMN [{EXTRACTED_DATE_COL}] DATE NOT NULL")
                    )
            except Exception as e:
                print(f"    !! Error during nullable check/alter for {table}: {e}")

def main():
    print(f"Connecting to {DB_TYPE} database...")
    tables = get_table_list(engine, DB_TYPE)
    for table in tables:
        ensure_nd_extracted_date_column_or_update(engine, table, DB_TYPE)

if __name__ == '__main__':
    main()