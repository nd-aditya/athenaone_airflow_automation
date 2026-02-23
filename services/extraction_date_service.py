"""
Add nd_extracted_date column to all tables in the incremental schema
and set it to the current date (extraction date).
"""
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from services.config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA


def get_mysql_engine():
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{INCREMENTAL_SCHEMA}",
        pool_pre_ping=True,
    )


def add_extraction_date_to_all_tables() -> dict:
    """
    Add nd_extracted_date DATE column to all tables in INCREMENTAL_SCHEMA if missing,
    then set nd_extracted_date = CURRENT_DATE() for all rows in each table.

    Returns a summary dict with tables_processed and any errors.
    """
    engine = get_mysql_engine()
    inspector = inspect(engine)
    schema_name = INCREMENTAL_SCHEMA

    results = {"tables_processed": 0, "columns_added": [], "errors": []}

    with engine.connect() as conn:
        tables = inspector.get_table_names(schema=schema_name)

        for table in tables:
            try:
                columns = inspector.get_columns(table, schema=schema_name)
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
                    results["columns_added"].append(table)

                conn.execute(
                    text(
                        f"""
                        UPDATE `{schema_name}`.`{table}`
                        SET `nd_extracted_date` = CURRENT_DATE();
                        """
                    )
                )
                conn.commit()
                results["tables_processed"] += 1

            except SQLAlchemyError as e:
                results["errors"].append({"table": table, "error": str(e)})

    if results["errors"]:
        raise RuntimeError(
            f"add_extraction_date failed for {len(results['errors'])} table(s): {results['errors']}"
        )

    return results
