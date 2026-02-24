from sqlalchemy import create_engine, MetaData, Table, select, insert
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import SQLAlchemyError
from multiprocessing import Pool, cpu_count

BATCH_SIZE = 10000  # read 10k rows at a time

def create_destination_table(conn_str, source_table, dest_table):
    engine = create_engine(conn_str)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    if source_table not in metadata.tables:
        raise Exception(f"Source table '{source_table}' not found.")

    src = metadata.tables[source_table]

    with engine.begin() as conn:
        if dest_table in metadata.tables:
            conn.execute(DropTable(metadata.tables[dest_table], if_exists=True))

        new_table = Table(dest_table, MetaData(), *[c.copy() for c in src.columns])
        new_table.create(bind=conn)
        print(f"Destination table '{dest_table}' created.")

def process_batch(args):
    conn_str, source_table, dest_table, offset, limit, seen_ids = args
    engine = create_engine(conn_str, pool_recycle=3600, pool_pre_ping=True)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    src = metadata.tables[source_table]
    dst = metadata.tables[dest_table]

    with engine.begin() as conn:
        stmt = select(src).offset(offset).limit(limit)
        rows = conn.execute(stmt).fetchall()

        for row in rows:
            nd_id = row._mapping['nd_auto_increment_id']
            if nd_id not in seen_ids:
                try:
                    conn.execute(insert(dst).values(row._mapping))
                    seen_ids.add(nd_id)
                except SQLAlchemyError as e:
                    print(f"Insert failed for ID={nd_id}: {e}")

def deduplicate_rows_parallel(conn_str, source_table, dest_table):
    create_destination_table(conn_str, source_table, dest_table)
    engine = create_engine(conn_str)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    src = metadata.tables[source_table]

    from sqlalchemy import func
    with engine.connect() as conn:
        total_rows = conn.execute(select(func.count()).select_from(src)).scalar()

    print(f"Total rows to scan: {total_rows}")

    seen_ids = set()
    offsets = range(0, total_rows, BATCH_SIZE)
    args_list = [(conn_str, source_table, dest_table, offset, BATCH_SIZE, seen_ids) for offset in offsets]

    # Use fewer processes to avoid memory issues with large sets
    with Pool(processes=max(cpu_count() - 2, 4)) as pool:
        pool.map(process_batch, args_list)

if __name__ == "__main__":
    conn = 'mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/deidentified'
    source_table = "DOCUMENT"
    dest_table = "DOCUMENT_nd_bak"
    deduplicate_rows_parallel(conn, source_table, dest_table)
