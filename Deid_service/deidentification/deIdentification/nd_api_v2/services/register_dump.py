from typing import Optional
from core.dbPkg.dbhandler import NDDBHandler
from nd_api_v2.models.table_details import TableMetadata, Table
from nd_api_v2.models.incremental_queue import IncrementalQueue
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from nd_api_v2.services.utils import get_default_table_details_for_ui

import threading

def ensure_table_metadata(table_name: str, connection_string: Optional[str] = None, columns: Optional[list[str]] = None, primary_key: Optional[list[str]] = None):
    # Case-insensitive lookup so existing config for e.g. "patientmedication" is reused
    # when the table is now registered as "PATIENTMEDICATION".
    table_metadata = TableMetadata.objects.filter(table_name__iexact=table_name).first()
    if table_metadata is not None:
        return table_metadata
    # Not found — create with uppercase name so all new registrations are consistent.
    nd_db_handler = NDDBHandler(connection_string)
    if columns is None:
        columns = nd_db_handler.get_column_names(table_name)
    if primary_key is None:
        primary_key = nd_db_handler.get_primary_key(table_name)
    table_metadata = TableMetadata(
        table_name=table_name.upper(),
        columns=columns,
        primary_key=primary_key,
        table_details_for_ui=get_default_table_details_for_ui(columns),
    )
    table_metadata.save()
    return table_metadata

def update_auto_increment_values(table_metadata: TableMetadata, table_obj: Table, max_rows: int):
    prev_value = table_metadata.max_nd_auto_increment_id
    new_row_count = prev_value + max_rows
    table_obj.nd_auto_increment_start_value = prev_value + 1
    table_obj.nd_auto_increment_end_value = new_row_count
    table_obj.save()
    table_metadata.max_nd_auto_increment_id = new_row_count
    table_metadata.save()

def register_dump_in_queue(connection_string: str, dump_date: str):
    nd_db_handler = NDDBHandler(connection_string)
    tables = nd_db_handler.get_all_tables()
    queue_name = f"queue_{dump_date}"
    incremental_queue, _ = IncrementalQueue.objects.get_or_create(
        queue_name=queue_name,
        dump_date=dump_date
    )

    threadlocal_handler = threading.local()

    def get_threadlocal_dbhandler():
        if not hasattr(threadlocal_handler, 'db_handler'):
            threadlocal_handler.db_handler = NDDBHandler(connection_string)
        return threadlocal_handler.db_handler

    def table_pipeline(table):
        db_handler = get_threadlocal_dbhandler()
        table = table.upper()
        # Reuse ensure_table_metadata
        table_metadata = ensure_table_metadata(table, connection_string)
        table_obj, _ = Table.register_table(table_metadata, incremental_queue)
        max_rows = db_handler.get_rows_count(table)
        # Reuse update_auto_increment_values
        update_auto_increment_values(table_metadata, table_obj, max_rows)
        return table

    results = []
    with ThreadPoolExecutor() as executor:
        tasks = {executor.submit(table_pipeline, table): table for table in tables}
        for future in tqdm(as_completed(tasks), total=len(tables), desc='Registering tables'):
            try:
                results.append(future.result())
            except Exception as e:
                import traceback
                print(f"Error registering table: {tasks[future]}: {e}")
                traceback.print_exc()

    return results, incremental_queue


def register_table_metadata(table_name: str, connection_string: Optional[str] = None, columns: Optional[list[str]] = None, primary_key: Optional[list[str]] = None):
    table_metadata = ensure_table_metadata(table_name, connection_string, columns, primary_key)
    return table_metadata


def register_table_and_add_to_queue(table_name: str, connection_string: str, queue: IncrementalQueue):
    table_metadata = TableMetadata.objects.filter(table_name=table_name).last()
    if table_metadata is None:
        table_metadata = register_table_metadata(table_name, connection_string)
    nd_db_handler = NDDBHandler(connection_string)
    max_rows = nd_db_handler.get_rows_count(table_name)
    table_obj, _ = Table.register_table(table_metadata, queue)
    update_auto_increment_values(table_metadata, table_obj, max_rows)
    return table_obj