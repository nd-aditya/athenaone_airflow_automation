"""
Copy data from historical schema into a date-stamped diff schema (diff_YYYYMMDD).
Creates the diff schema if it doesn't exist. Only copies rows where nd_extracted_date = CURDATE().
"""
import time
from datetime import date

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    HISTORICAL_SCHEMA,
)


def _q(name: str) -> str:
    """Quote identifier with backticks."""
    return f"`{name}`"


def copy_historical_to_diff_schema() -> dict:
    """
    Create schema diff_<current_date> if not exists, then for each table in
    HISTORICAL_SCHEMA: create table in diff schema and insert only rows
    where nd_extracted_date = CURDATE().

    Returns summary dict for XCom.
    """
    diff_schema = f"diff_{date.today().strftime('%Y%m%d')}"
    connection_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    engine = create_engine(connection_str, pool_pre_ping=True)
    inspector = inspect(engine)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {_q(diff_schema)}"))
        conn.commit()

    hist_tables = inspector.get_table_names(schema=HISTORICAL_SCHEMA)
    results = []
    start_all = time.time()

    for table_name in hist_tables:
        stats = {
            "table": table_name,
            "inserted": 0,
            "duration": None,
            "error": None,
        }
        start = time.time()
        try:
            hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
            diff_fqn = f"{_q(diff_schema)}.{_q(table_name)}"

            with engine.begin() as conn:
                create_sql = f"CREATE TABLE {diff_fqn} LIKE {hist_fqn};"
                conn.execute(text(create_sql))

                columns = [c["name"] for c in inspector.get_columns(table_name, schema=HISTORICAL_SCHEMA)]
                has_nd_extracted_date = "nd_extracted_date" in [c.lower() for c in columns]

                if has_nd_extracted_date:
                    insert_sql = (
                        f"INSERT INTO {diff_fqn} "
                        f"SELECT * FROM {hist_fqn} WHERE nd_extracted_date = CURDATE()"
                    )
                else:
                    insert_sql = f"INSERT INTO {diff_fqn} SELECT * FROM {hist_fqn}"

                result = conn.execute(text(insert_sql))
                stats["inserted"] = result.rowcount if result.rowcount is not None else 0

            stats["duration"] = round(time.time() - start, 3)
            results.append(stats)
        except SQLAlchemyError as e:
            stats["error"] = f"{type(e).__name__}: {str(e)}"
            stats["duration"] = round(time.time() - start, 3)
            results.append(stats)

    total_time = round(time.time() - start_all, 2)
    total_inserted = sum(r.get("inserted", 0) for r in results)
    failed = [r for r in results if r.get("error")]

    summary = {
        "diff_schema": diff_schema,
        "total_tables": len(results),
        "succeeded": len(results) - len(failed),
        "failed": len(failed),
        "total_rows_inserted": total_inserted,
        "total_time_seconds": total_time,
        "failed_tables": [{"table": r["table"], "error": r["error"]} for r in failed],
        "per_table_results": results,
    }

    return summary
