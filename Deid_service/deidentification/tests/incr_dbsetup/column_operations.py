import sys
from sqlalchemy import create_engine, text, inspect
import pandas as pd

# Config
CONN_STR = "mysql+mysqlconnector://root:123456789@localhost:3306/automation_incr_testing_diff_28nov2025"  # Change as needed
DB_TYPE = "mysql"  # 'mysql' or 'mssql'
OPERATION_TYPE = "drop"  # 'drop' or 'rename'
COLUMN_NAME = "nd_auto_increment_id"  # Column name to drop or rename
NEW_COL_NAME = "nd_auto_increment_id_old"  # Only used for rename

if OPERATION_TYPE not in ("drop", "rename"):
    print("OPERATION_TYPE must be either 'drop' or 'rename'.")
    sys.exit(1)
if OPERATION_TYPE == "rename":
    if not NEW_COL_NAME:
        print("Set the new column name in the NEW_COL_NAME variable!")
        sys.exit(1)

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

def column_exists(engine, db_type, table, column):
    insp = inspect(engine)
    try:
        cols = [c['name'] if isinstance(c, dict) else c['name'] for c in insp.get_columns(table)]
        return column in cols
    except Exception as e:
        print(f"  - Skipping table {table}, error: {e}")
        return False

def drop_column(engine, db_type, table, column):
    print(f"  - Processing table '{table}'...")
    if column_exists(engine, db_type, table, column):
        print(f"    > Dropping column '{column}' from '{table}'")
        with engine.begin() as conn:
            if db_type == "mysql":
                conn.execute(text(f'ALTER TABLE `{table}` DROP COLUMN `{column}`'))
            else:
                conn.execute(text(f'ALTER TABLE [{table}] DROP COLUMN [{column}]'))
    else:
        print(f"    > Column '{column}' does not exist in '{table}', skipping.")

def rename_column(engine, db_type, table, old_col, new_col):
    print(f"  - Processing table '{table}'...")
    if column_exists(engine, db_type, table, old_col):
        print(f"    > Renaming column '{old_col}' to '{new_col}' in '{table}'")
        with engine.begin() as conn:
            if db_type == "mysql":
                # Need the type of the existing column to do the rename in MySQL
                insp = inspect(engine)
                cols = [c for c in insp.get_columns(table) if c['name'] == old_col]
                if not cols:
                    print(f"    > Could not determine type for column '{old_col}', skipping.")
                    return
                coltype = cols[0]['type']
                nullable = "NULL" if cols[0].get('nullable', True) else "NOT NULL"
                # Assemble the new definition
                mysql_rename_sql = f'ALTER TABLE `{table}` CHANGE `{old_col}` `{new_col}` {coltype} {nullable}'
                conn.execute(text(mysql_rename_sql))
            else:
                # SQL Server syntax
                conn.execute(text(f'EXEC sp_rename \'{table}.{old_col}\', \'{new_col}\', \'COLUMN\''))
    else:
        print(f"    > Column '{old_col}' does not exist in '{table}', skipping.")

def main():
    print(f"Connecting to {DB_TYPE} database...")
    tables = get_table_list(engine, DB_TYPE)
    if OPERATION_TYPE == "drop":
        for table in tables:
            drop_column(engine, DB_TYPE, table, COLUMN_NAME)
    elif OPERATION_TYPE == "rename":
        for table in tables:
            rename_column(engine, DB_TYPE, table, COLUMN_NAME, NEW_COL_NAME)
    print("Done.")

if __name__ == '__main__':
    main()
