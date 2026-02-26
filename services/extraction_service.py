from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pandas as pd
import os

from services.config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    INCREMENTAL_SCHEMA,
    HISTORICAL_SCHEMA,
    CONTEXT_IDS,
    BATCH_SIZE,
)

MYSQL_DATABASE = INCREMENTAL_SCHEMA

SCHEMA = "ATHENAONE"


# --------------------------------------------
# FULL ORIGINAL DATA TYPE MAPPING (UNCHANGED)
# --------------------------------------------

data_type_mapping = {
   'BOOLEAN': 'TINYINT(1)',
   'DATE': 'DATE',
   'FLOAT': 'FLOAT',

   'NUMBER(1,0)': 'TINYINT',
   'NUMBER(2,0)': 'TINYINT',
   'NUMBER(3,0)': 'TINYINT',
   'NUMBER(4,0)': 'SMALLINT',
   'NUMBER(5,0)': 'INT',
   'NUMBER(6,0)': 'MEDIUMINT',
   'NUMBER(7,0)': 'MEDIUMINT',
   'NUMBER(8,0)': 'INT',
   'NUMBER(10,0)': 'INT',
   'NUMBER(11,0)': 'INT',
   'NUMBER(12,0)': 'BIGINT',
   'NUMBER(13,0)': 'BIGINT',
   'NUMBER(14,4)': 'DECIMAL(14,4)',
   'NUMBER(16,0)': 'BIGINT',
   'NUMBER(18,0)': 'BIGINT',
   'NUMBER(19,0)': 'BIGINT',
   'NUMBER(20,2)': 'DECIMAL(20,2)',
   'NUMBER(20,8)': 'DECIMAL(20,8)',
   'NUMBER(21,5)': 'DECIMAL(21,5)',
   'NUMBER(22,0)': 'BIGINT',
   'NUMBER(22,2)': 'DECIMAL(22,2)',
   'NUMBER(22,3)': 'DECIMAL(22,3)',
   'NUMBER(24,6)': 'DECIMAL(24,6)',
   'NUMBER(28,8)': 'DECIMAL(28,8)',
   'NUMBER(30,0)': 'DECIMAL(30,0)',
   'NUMBER(32,2)': 'DECIMAL(32,2)',
   'NUMBER(38,0)': 'DECIMAL(38,0)',
   'NUMBER(38,5)': 'DECIMAL(38,5)',
   'NUMBER(38,10)': 'DECIMAL(38,10)',

   'NUMBER(4,2)': 'DECIMAL(4,2)',
   'NUMBER(5,2)': 'DECIMAL(5,2)',
   'NUMBER(5,3)': 'DECIMAL(5,3)',
   'NUMBER(8,2)': 'DECIMAL(8,2)',
   'NUMBER(8,3)': 'DECIMAL(8,3)',
   'NUMBER(8,4)': 'DECIMAL(8,4)',
   'NUMBER(8,6)': 'DECIMAL(8,6)',
   'NUMBER(10,2)': 'DECIMAL(10,2)',
   'NUMBER(10,4)': 'DECIMAL(10,4)',
   'NUMBER(10,6)': 'DECIMAL(10,6)',
   'NUMBER(11,2)': 'DECIMAL(11,2)',
   'NUMBER(11,3)': 'DECIMAL(11,3)',
   'NUMBER(12,1)': 'DECIMAL(12,1)',
   'NUMBER(12,2)': 'DECIMAL(12,2)',
   'NUMBER(12,3)': 'DECIMAL(12,3)',
   'NUMBER(12,4)': 'DECIMAL(12,4)',
   'NUMBER(12,6)': 'DECIMAL(12,6)',
   'NUMBER(17,5)': 'DECIMAL(17,5)',
   'NUMBER(18,5)': 'DECIMAL(18,5)',
   'NUMBER(18,6)': 'DECIMAL(18,6)',

   'TIMESTAMP_NTZ(9)': 'DATETIME',

   'VARCHAR(1)': 'VARCHAR(1)',
   'VARCHAR(2)': 'VARCHAR(2)',
   'VARCHAR(6)': 'VARCHAR(6)',
   'VARCHAR(7)': 'VARCHAR(7)',
   'VARCHAR(10)': 'VARCHAR(10)',
   'VARCHAR(11)': 'VARCHAR(11)',
   'VARCHAR(12)': 'VARCHAR(12)',
   'VARCHAR(13)': 'VARCHAR(13)',
   'VARCHAR(18)': 'VARCHAR(18)',
   'VARCHAR(20)': 'VARCHAR(20)',
   'VARCHAR(28)': 'VARCHAR(28)',
   'VARCHAR(30)': 'VARCHAR(30)',
   'VARCHAR(50)': 'VARCHAR(50)',
   'VARCHAR(16777216)': 'TEXT',
}


# --------------------------------------------
# Helpers
# --------------------------------------------

def get_date_range(table_name: str | None = None):
    """
    Get (start_date, end_date) for extraction.
    If table_name is provided, start_date is max(LASTUPDATED) from that table in the
    historical MySQL schema (athenaone) so no data is lost between runs; otherwise start = today - 3 days.
    """
    fallback_start = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    end = datetime.now().strftime("%Y-%m-%d")

    if table_name:
        try:
            hist_engine = get_historical_mysql_engine()
            with hist_engine.connect() as conn:
                row = conn.execute(
                    text(f"SELECT MAX(`LASTUPDATED`) AS max_ts FROM `{table_name}`")
                ).fetchone()
            if row and row[0] is not None:
                max_ts = row[0]
                if hasattr(max_ts, "strftime"):
                    start = max_ts.strftime("%Y-%m-%d")
                else:
                    start = str(max_ts)[:10]
                return start, end
        except Exception:
            pass
        start = fallback_start
    else:
        start = fallback_start

    return start, end


def get_snowflake_engine():
    return create_engine(
        f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
        f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SCHEMA}"
        f"?warehouse={SNOWFLAKE_WAREHOUSE}",
        connect_args={"insecure_mode": True},
        pool_pre_ping=True,
    )


def get_mysql_engine():
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}/{MYSQL_DATABASE}",
        pool_pre_ping=True,
    )


def get_historical_mysql_engine():
    """MySQL engine for the historical schema (athenaone). Used for max(LASTUPDATED) in get_date_range."""
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}/{HISTORICAL_SCHEMA}",
        pool_pre_ping=True,
    )


def _escape_row(row):
    """
    Escape % characters in string values to prevent PyMySQL from
    misinterpreting them as format string placeholders during executemany.
    """
    return tuple(
        val.replace('%', '%%') if isinstance(val, str) else val
        for val in row
    )


# --------------------------------------------
# Core Extraction
# --------------------------------------------

def extract_table(table_name: str):

    snowflake_engine = get_snowflake_engine()
    mysql_engine = get_mysql_engine()

    start_date, end_date = get_date_range(table_name=table_name)

    desc_query = f"DESC VIEW {SNOWFLAKE_DATABASE}.{SCHEMA}.{table_name};"

    with snowflake_engine.connect() as conn:
        result = conn.execute(text(desc_query))
        columns_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    columns_sql = []
    column_names = []

    for _, row in columns_df.iterrows():
        col_name = row["name"]
        snowflake_type = row["type"]
        mysql_type = data_type_mapping.get(snowflake_type, "TEXT")

        columns_sql.append(f"`{col_name}` {mysql_type}")
        column_names.append(col_name)

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns_sql)}
        );
    """

    with mysql_engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

    where_clause = (
        f"contextid IN {CONTEXT_IDS} "
        f"AND LASTUPDATED >= '{start_date}' "
        f"AND LASTUPDATED < '{end_date}'"
    )

    select_query = (
        f"SELECT * FROM {SNOWFLAKE_DATABASE}.{SCHEMA}.{table_name} "
        f"WHERE {where_clause}"
    )

    with snowflake_engine.connect() as conn:
        result = conn.execute(text(select_query))
        data = result.fetchall()

    if not data:
        return {"table": table_name, "rows_inserted": 0, "status": "no_data"}

    columns = ", ".join([f"`{col}`" for col in column_names])
    placeholders = ", ".join(["%s"] * len(column_names))

    insert_query = f"""
        INSERT INTO `{table_name}` ({columns})
        VALUES ({placeholders})
    """

    raw_conn = mysql_engine.raw_connection()
    cursor = raw_conn.cursor()

    try:
        for i in range(0, len(data), BATCH_SIZE):
            batch = [_escape_row(row) for row in data[i:i + BATCH_SIZE]]
            cursor.executemany(insert_query, batch)

        raw_conn.commit()

    except Exception:
        raw_conn.rollback()
        raise

    finally:
        cursor.close()
        raw_conn.close()

    return {
        "table": table_name,
        "rows_inserted": len(data),
        "status": "success",
    }