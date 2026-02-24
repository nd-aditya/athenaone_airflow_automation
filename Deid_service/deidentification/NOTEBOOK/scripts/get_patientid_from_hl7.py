from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
import re

THREAD_COUNT = 8
BATCH_SIZE = 5000

def extract_hl7_value(message, segment, field_num):
    """
    Extract value of a specific HL7 segment-field (e.g., PID-3).
    """
    if not message:
        return None
    try:
        lines = message.splitlines()
        for line in lines:
            if line.startswith(segment + '|'):
                fields = line.split('|')
                return fields[field_num - 1].strip() if len(fields) >= field_num else None
    except Exception:
        return None
    return None

def extract_hl7_and_insert_parallel(
    connection_string,
    table_name,
    new_table_name,
    hl7_column,
    segment,
    field_num,
    new_column_name,
    id_column='id'
):
    engine = create_engine(connection_string)

    # Step 1: Create new table with additional column
    with engine.begin() as conn:
        conn.execute(text(f"IF OBJECT_ID('{new_table_name}', 'U') IS NOT NULL DROP TABLE {new_table_name}"))
        sample = conn.execute(text(f"SELECT TOP 0 * FROM {table_name}"))
        base_columns = sample.keys()
        all_columns = list(base_columns) + [new_column_name]
        create_cols = ", ".join([f"[{col}] VARCHAR(MAX)" for col in base_columns] + [f"[{new_column_name}] VARCHAR(100)"])
        conn.execute(text(f"CREATE TABLE {new_table_name} ({create_cols})"))

        # Step 2: Get all IDs
        id_rows = conn.execute(text(f"SELECT [{id_column}] FROM {table_name}")).fetchall()
        all_ids = [row[0] for row in id_rows]

    chunks = [all_ids[i:i+BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]

    def process_chunk(id_chunk):
        thread_engine = create_engine(connection_string)
        with thread_engine.begin() as conn:
            id_list = ", ".join(str(i) for i in id_chunk)
            rows = conn.execute(text(f"SELECT * FROM {table_name} WHERE {id_column} IN ({id_list})")).fetchall()
            if not rows:
                return
            col_names = rows[0].keys()
            results = []
            for row in rows:
                row_dict = dict(zip(col_names, row))
                hl7_msg = row_dict.get(hl7_column)
                hl7_value = extract_hl7_value(hl7_msg, segment, field_num)
                row_dict[new_column_name] = hl7_value
                results.append(row_dict)
            if results:
                placeholders = ", ".join([f":{key}" for key in results[0].keys()])
                sql = f"INSERT INTO {new_table_name} ({', '.join(f'[{k}]' for k in results[0].keys())}) VALUES ({placeholders})"
                conn.execute(text(sql), results)

    # Step 3: Parallel execution
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for f in futures:
            f.result()

    print(f"✅ Done: {len(all_ids)} rows processed and inserted into {new_table_name}")


extract_hl7_and_insert_parallel(
    connection_string="mssql+pymssql://username:password@localhost:1433/your_database",
    table_name="original_table_name",
    new_table_name="new_table_with_extracted_patient_id",
    hl7_column="hl7_message_column",      # column that contains HL7 message strings
    segment="PID",                        # HL7 segment to extract from, e.g. "PID"
    field_num=3,                          # HL7 field number (e.g. PID-3 is field 3)
    new_column_name="PATIENT_ID",         # name for the new column in the new table
    id_column="primary_key_column_name"   # name of a unique ID column (e.g. "id", "nd_auto_increment_id")
)