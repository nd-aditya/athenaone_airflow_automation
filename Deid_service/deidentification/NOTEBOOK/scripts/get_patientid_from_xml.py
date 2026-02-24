from sqlalchemy import create_engine, text
from xml.etree import ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import re

# Configurable constants
THREAD_COUNT = 8
BATCH_SIZE = 5000

def extract_tag_values_and_insert_parallel(
    connection_string,
    table_name,
    new_table_name,
    xml_column,
    tag_name,
    new_column_name,
    id_column="id"  # default PK or unique column
):
    engine = create_engine(connection_string)

    with engine.begin() as conn:
        # Drop + Create new table
        conn.execute(text(f"IF OBJECT_ID('{new_table_name}', 'U') IS NOT NULL DROP TABLE {new_table_name}"))
        sample_result = conn.execute(text(f"SELECT TOP 0 * FROM {table_name}"))
        columns = sample_result.keys()
        new_columns = list(columns) + [new_column_name]
        column_defs = ", ".join([f"[{c}] VARCHAR(MAX)" for c in columns] + [f"[{new_column_name}] VARCHAR(100)"])
        conn.execute(text(f"CREATE TABLE {new_table_name} ({column_defs})"))

        # Get all IDs
        id_rows = conn.execute(text(f"SELECT [{id_column}] FROM {table_name}")).fetchall()
        all_ids = [row[0] for row in id_rows]

    # Split into chunks
    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, len(all_ids), BATCH_SIZE)]

    def process_chunk(id_chunk):
        thread_engine = create_engine(connection_string)
        with thread_engine.begin() as conn:
            results = []
            id_list_str = ", ".join(str(i) for i in id_chunk)
            result = conn.execute(text(f"SELECT * FROM {table_name} WHERE {id_column} IN ({id_list_str})"))
            rows = result.fetchall()
            col_names = result.keys() if rows else []

            for row in rows:
                row_dict = dict(zip(col_names, row))
                xml = row_dict.get(xml_column)
                tag_val = extract_xml_tag_value(xml, tag_name)
                row_dict[new_column_name] = tag_val
                results.append(row_dict)

            if results:
                placeholders = ", ".join(f":{key}" for key in results[0].keys())
                insert_sql = f"INSERT INTO {new_table_name} ({', '.join(f'[{k}]' for k in results[0].keys())}) VALUES ({placeholders})"
                conn.execute(text(insert_sql), results)

    # Run multithreaded extraction/insertion
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for f in futures:
            f.result()  # raise exceptions if any

    print(f"✅ Done: Inserted {len(all_ids)} rows into {new_table_name}")


def extract_xml_tag_value(xml_str, tag):
    if not xml_str:
        return None
    xml_str = _strip_cdata(xml_str)

    # Try XML parsing
    try:
        root = ET.fromstring(xml_str)
        for elem in root.iter():
            tag_name = elem.tag.split('}')[-1]
            if tag_name.lower() == tag.lower():
                return elem.text.strip() if elem.text else None
    except ET.ParseError:
        # Fall back to regex if XML is broken
        return extract_using_regex(xml_str, tag)

    return None

def _strip_cdata(xml):
    match = re.search(r'<!\[CDATA\[(.*?)\]\]>', xml, re.DOTALL)
    content = match.group(1) if match else xml
    content = re.sub(r'<\?xml[^>]+\?>', '', content)
    content = re.sub(r'<\?xml-stylesheet[^>]+\?>', '', content)
    return content.strip()

def extract_using_regex(xml, tag):
    pattern = rf"<{tag}[^>]*>(.*?)</{tag}>"
    match = re.search(pattern, xml, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


# 🔧 Usage
extract_tag_values_and_insert_parallel(
    connection_string="mssql+pymssql://sa:ndADMIN2025@localhost:1433/db_masnin",
    table_name="emrereferralattachments",
    new_table_name="emrereferralattachments_with_patient_id",
    xml_column="progressnotes",
    tag_name="PatientId",
    new_column_name="PATIENT_ID",
    id_column="nd_auto_increment_id"  # PK or unique column
)
