import sys
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Config
CONN_STR = "mysql+mysqlconnector://root:123456789@localhost:3306/automation_incr_testing"  # Change as needed
DB_TYPE = "mysql"  # 'mysql' or 'mssql'
AUTO_INC_COL = "nd_auto_increment_id"

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

def column_exists_and_correct(engine, table, db_type):
    if db_type == "mysql":
        sql = """
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY, EXTRA
        FROM information_schema.columns
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s
        """
        with engine.begin() as conn:
            result = conn.execute(sql, (table, AUTO_INC_COL))
            row = result.fetchone()
        if not row:
            return False
        # Must be BIGINT, UNIQUE or PRIMARY KEY
        if not (row['DATA_TYPE'].lower() == "bigint"):
            return False
        return True
    else:
        # mssql
        sql = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
        """
        with engine.begin() as conn:
            result = conn.execute(sql, (table, AUTO_INC_COL))
            row = result.fetchone()
        if not row:
            return False
        if row.DATA_TYPE.lower() != "bigint":
            return False
        return True

def ensure_nd_auto_increment_id(engine, table, db_type):
    insp = inspect(engine)
    cols = [c['name'] if isinstance(c, dict) else c['name'] for c in insp.get_columns(table)]

    if AUTO_INC_COL in cols:
        # Check type
        if not column_exists_and_correct(engine, table, db_type):
            print(f"  - Column exists on {table} but is not defined correctly. Dropping it...")
            with engine.begin() as conn:
                if db_type=='mysql':
                    conn.execute(text(f"ALTER TABLE `{table}` DROP COLUMN `{AUTO_INC_COL}`"))
                else:
                    conn.execute(text(f'ALTER TABLE "{table}" DROP COLUMN "{AUTO_INC_COL}"'))
        else:
            print(f"  - Table '{table}': column already present.")
            return

    # Add column
    print(f"  - Adding '{AUTO_INC_COL}' to '{table}'...")
    if db_type == "mysql":
        alter = f'ALTER TABLE `{table}` ADD COLUMN `{AUTO_INC_COL}` BIGINT'
        with engine.begin() as conn:
            conn.execute(text(alter))
    else:
        alter = f'ALTER TABLE [{table}] ADD [{AUTO_INC_COL}] BIGINT'
        with engine.begin() as conn:
            conn.execute(text(alter))
    # Fill (row_number)
    print(f"  - Populating '{AUTO_INC_COL}' for {table}...")
    if db_type == "mysql":
        with engine.begin() as conn:
            conn.execute(text(f"SET @rownum=0"))
            upq = f'UPDATE `{table}` SET `{AUTO_INC_COL}` = (@rownum := @rownum + 1)'
            conn.execute(text(upq))
        uq = f'ALTER TABLE `{table}` ADD UNIQUE (`{AUTO_INC_COL}`)'
        with engine.begin() as conn:
            conn.execute(text(uq))
    else:
        with engine.begin() as conn:
            # Add temp ordinal col
            # On SQLServer, use ROW_NUMBER()
            update_sql = f"""
            WITH Numbered AS (
                SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
                FROM [{table}]
            )
            UPDATE N
            SET N.[{AUTO_INC_COL}] = N.rn
            FROM [{table}] T
            INNER JOIN Numbered N ON N.[AUTO_INC_COL] IS NULL AND T.[AUTO_INC_COL] IS NULL AND N.rn = T.rn
            """
            # Remove join ambiguity: update using ROW_NUMBER()
            # In practice, just set using row_number partition
            alter_nulls = f"UPDATE [{table}] SET [{AUTO_INC_COL}] = rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as rn, * FROM [{table}]) as asub WHERE [{table}].[{AUTO_INC_COL}] IS NULL"
            # Simpler way:
            conn.execute(text(f"DROP TABLE IF EXISTS #upd_{table}"))
            conn.execute(text(f"SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn, * INTO #upd_{table} FROM [{table}]"))
            conn.execute(text(f"UPDATE t SET t.[{AUTO_INC_COL}] = u.rn FROM [{table}] t INNER JOIN #upd_{table} u ON t.[{AUTO_INC_COL}] IS NULL AND t.<primarykey>=u.<primarykey>"))
            conn.execute(text(f"DROP TABLE #upd_{table}"))
        # Add UNIQUE constraint
        with engine.begin() as conn:
            conn.execute(text(f'CREATE UNIQUE INDEX IX_{table}_{AUTO_INC_COL} ON [{table}]([{AUTO_INC_COL}])'))

def main():
    print(f"Connecting to {DB_TYPE} database...")
    tables = get_table_list(engine, DB_TYPE)
    for table in tables:
        ensure_nd_auto_increment_id(engine, table, DB_TYPE)

if __name__ == '__main__':
    main()