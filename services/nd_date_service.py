"""
Add nd_extracted_date column to all tables in the incremental schema
and set it to the current date (extraction date).
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from services.config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA

ADD_ND_DATE_MAX_WORKERS = 10


def get_mysql_engine(pool_size: int = 1, max_overflow: int = 0):
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{INCREMENTAL_SCHEMA}",
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
    )


def _add_extraction_date_one_table(engine, schema_name: str, table: str) -> dict:
    """
    Add nd_extracted_date to one table if missing, then set to CURRENT_DATE().
    Returns {"table": table, "column_added": bool, "error": None or str}.
    """
    out = {"table": table, "column_added": False, "error": None}
    try:
        with engine.connect() as conn:
            columns = inspect(engine).get_columns(table, schema=schema_name)
            column_names = [col["name"].lower() for col in columns]
            if "nd_extracted_date" not in column_names:
                conn.execute(
                    text(
                        f"""
                        ALTER TABLE `{schema_name}`.`{table}`
                        ADD COLUMN `nd_extracted_date` DATE;
                        """
                    )
                )
                conn.commit()
                out["column_added"] = True
            conn.execute(
                text(
                    f"""
                    UPDATE `{schema_name}`.`{table}`
                    SET `nd_extracted_date` = CURRENT_DATE();
                    """
                )
            )
            conn.commit()
    except SQLAlchemyError as e:
        out["error"] = str(e)
    return out


def add_extraction_date_to_all_tables() -> dict:
    """
    Add nd_extracted_date DATE column to all tables in INCREMENTAL_SCHEMA if missing,
    then set nd_extracted_date = CURRENT_DATE() for all rows in each table.
    Runs per-table work in parallel (ADD_ND_DATE_MAX_WORKERS).

    Returns a summary dict with tables_processed and any errors.
    """
    engine = get_mysql_engine(
        pool_size=ADD_ND_DATE_MAX_WORKERS,
        max_overflow=2,
    )
    inspector = inspect(engine)
    schema_name = INCREMENTAL_SCHEMA
    tables = inspector.get_table_names(schema=schema_name)

    results = {"tables_processed": 0, "columns_added": [], "errors": []}

    with ThreadPoolExecutor(max_workers=ADD_ND_DATE_MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(_add_extraction_date_one_table, engine, schema_name, table): table
            for table in tables
        }
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                one = future.result()
                if one.get("error"):
                    results["errors"].append({"table": one["table"], "error": one["error"]})
                else:
                    results["tables_processed"] += 1
                    if one.get("column_added"):
                        results["columns_added"].append(one["table"])
            except Exception as e:
                results["errors"].append({"table": table, "error": str(e)})

    engine.dispose()

    if results["errors"]:
        raise RuntimeError(
            f"add_extraction_date failed for {len(results['errors'])} table(s): {results['errors']}"
        )

    return results
