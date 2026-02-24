# script copy the all the rows from source db to dest db, (prepared to move mssql -> mysql move)
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# === CONFIGURATION ===
MSSQL_CONN_STRING = ""
MYSQL_CONN_STRING = ""

CHUNK_SIZE = 5000               # Rows per chunk
PROGRESS_FILE = "progress.json"

# === SQLAlchemy Engines ===
mssql_engine = create_engine(MSSQL_CONN_STRING)
mysql_engine = create_engine(MYSQL_CONN_STRING)

# === Map SQL Server types to MySQL types ===
def map_sqlserver_to_mysql(sql_type: str, max_len: int, precision: int, scale: int, observed_max_length: int = None) -> str:
    sql_type = sql_type.lower()
    if sql_type in ["int", "smallint", "bigint", "tinyint"]:
        return "INT"
    elif sql_type in ["numeric", "decimal", "money", "smallmoney"]:
        return f"DECIMAL({precision},{scale})"
    elif sql_type in ["float", "real"]:
        return "FLOAT"
    elif sql_type == "bit":
        return "TINYINT(1)"
    elif sql_type in ["datetime", "smalldatetime", "date", "time", "datetime2"]:
        return "DATETIME"
    elif sql_type in ["char", "nchar", "varchar", "nvarchar", "text", "ntext"]:
        if observed_max_length and observed_max_length > 65535:
            return "LONGTEXT"
        if max_len == -1 or max_len > 65535:
            return "LONGTEXT"
        else:
            return f"VARCHAR({max_len})"
    else:
        return "TEXT"

# === Load schema from MSSQL ===
def load_table_schema(engine: Engine, table_name: str):
    query = f"""
    SELECT c.name AS column_name,
           ty.name AS data_type,
           c.max_length,
           c.precision,
           c.scale
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = '{table_name.split('.')[-1]}' AND s.name = '{table_name.split('.')[0]}'
    ORDER BY c.column_id;
    """
    return pd.read_sql(query, engine)

# === Create table dynamically from the first chunk ===
def create_mysql_table_from_chunk(engine: Engine, table_name: str, schema_df: pd.DataFrame, chunk: pd.DataFrame):
    col_defs = []
    for _, row in schema_df.iterrows():
        col_name = row["column_name"]
        max_len = row["max_length"]
        precision = row["precision"]
        scale = row["scale"]
        observed_max_length = chunk[col_name].astype(str).map(len).max()
        data_type = map_sqlserver_to_mysql(row["data_type"], max_len, precision, scale, observed_max_length)
        col_defs.append(f"`{col_name}` {data_type}")
    create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    print(f"✅ Table {table_name} created successfully in MySQL.")

# === Insert chunk into MySQL ===
def insert_chunk(engine: Engine, table_name: str, chunk: pd.DataFrame):
    chunk.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"✅ Inserted {len(chunk)} rows into {table_name}.")

# === Load progress ===
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {}

# === Save progress ===
def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=4)

# === Main process with resume capability ===
def move_table(source_engine: Engine, target_engine: Engine, source_table: str, target_table: str):
    progress = load_progress()
    last_processed = progress.get(source_table, {}).get("last_processed", 0)
    table_created = progress.get(source_table, {}).get("table_created", False)

    try:
        schema_df = load_table_schema(source_engine, source_table)
        if schema_df.empty:
            print(f"❌ No schema found for {source_table}")
            return

        chunk_iter = pd.read_sql(f"SELECT * FROM {source_table} ORDER BY (SELECT NULL)", source_engine, chunksize=CHUNK_SIZE)
        total_rows = 0
        chunk_index = 0

        for chunk in chunk_iter:
            chunk_index += 1
            start_index = (chunk_index - 1) * CHUNK_SIZE

            if start_index <= last_processed:
                print(f"➡ Skipping chunk {chunk_index} (already processed)")
                continue

            if not table_created:
                with target_engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
                    print(f"✅ Dropped existing table {target_table} if it existed.")
                create_mysql_table_from_chunk(target_engine, target_table, schema_df, chunk)
                table_created = True
                # Save immediately after creating the table
                progress[source_table] = {"last_processed": last_processed, "table_created": True}
                save_progress(progress)

            insert_chunk(target_engine, target_table, chunk)
            last_processed = start_index + len(chunk) - 1
            total_rows += len(chunk)

            # Save progress after each chunk
            progress[source_table] = {"last_processed": last_processed, "table_created": table_created}
            save_progress(progress)

        print(f"✅ Finished moving {total_rows} rows from {source_table} to {target_table}.")

    except Exception as e:
        print(f"❌ Failed to move table {source_table}: {e}")
        print("➡ Progress saved. You can rerun the script to resume.")

# === Run ===
if __name__ == "__main__":
    mapping = {
        "dbo.DOCDATA2_with_patient_id" : "DOCDATA2_with_patient_id",
    }
    for key, value in mapping.items():
        move_table(mssql_engine, mysql_engine, key, value)
