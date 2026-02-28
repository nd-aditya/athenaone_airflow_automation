"""
Copy data from historical schema into a date-stamped diff schema (diff_YYYYMMDD).
Creates the diff schema if it doesn't exist. For each table, copies rows where
nd_extracted_date > max(nd_extracted_date) for that same table in DEIDENTIFIED_SCHEMA
(per-table cutoff to avoid data miss). If a table has no row in DEIDENTIFIED_SCHEMA,
uses date.min so all historical rows for that table are copied.

When tables_to_copy is provided (from DAG), only those tables are copied.
When None, only tables that have nd_extracted_date in historical are considered.

Also provides: update_diff_schema_history_and_drop_old() to keep a history file of
the last N diff/deid schema runs and drop older MySQL schemas.
"""
import json
import os
import time
from datetime import date
from typing import Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    HISTORICAL_SCHEMA,
    DEIDENTIFIED_SCHEMA,
)

# Default path for the history file (repo root / airflow_home / diff_schema_history.json)
def _default_history_path() -> str:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(root, "airflow_home", "diff_schema_history.json")


def _q(name: str) -> str:
    """Quote identifier with backticks."""
    return f"`{name}`"


def _get_max_nd_extracted_date_for_table(engine, schema: str, table_name: str) -> Optional[date]:
    """
    Return MAX(nd_extracted_date) for the given table in the given schema, or None
    if the table/column is missing or empty. Used as per-table cutoff when copying
    from historical so each table only copies rows newer than its own last deid.
    """
    inspector = inspect(engine)
    try:
        columns = [c["name"] for c in inspector.get_columns(table_name, schema=schema)]
    except Exception:
        return None
    if "nd_extracted_date" not in [c.lower() for c in columns]:
        return None
    try:
        fqn = f"{_q(schema)}.{_q(table_name)}"
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT MAX(`nd_extracted_date`) FROM {fqn}")).fetchone()
        if row and row[0] is not None:
            d = row[0] if isinstance(row[0], date) else row[0].date() if hasattr(row[0], "date") else None
            return d
    except Exception:
        pass
    return None


def copy_historical_to_diff_schema(tables_to_copy: Optional[list[str]] = None) -> dict:
    """
    Create schema diff_<current_date> if not exists, then copy from historical.
    For each table, only rows with nd_extracted_date > max(nd_extracted_date) for
    that same table in DEIDENTIFIED_SCHEMA are copied (per-table cutoff). If the
    table has no data in DEIDENTIFIED_SCHEMA, date.min is used so all rows are copied.

    Args:
        tables_to_copy: If provided, only these tables are copied. Must exist in HISTORICAL_SCHEMA.
            If None, all tables in historical that have nd_extracted_date are processed.

    Returns summary dict for XCom (includes "tables_to_copy" when tables_to_copy was provided).
    """
    diff_schema = f"diff_{date.today().strftime('%Y%m%d')}"
    connection_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    engine = create_engine(connection_str, pool_pre_ping=True)
    inspector = inspect(engine)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {_q(diff_schema)}"))
        conn.commit()

    hist_tables_all = inspector.get_table_names(schema=HISTORICAL_SCHEMA)
    if tables_to_copy is not None:
        tables_to_process = [t for t in tables_to_copy if t in hist_tables_all]
    else:
        tables_to_process = [
            t for t in hist_tables_all
            if "nd_extracted_date" in [c["name"].lower() for c in inspector.get_columns(t, schema=HISTORICAL_SCHEMA)]
        ]

    results = []
    start_all = time.time()

    for table_name in tables_to_process:
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
                    cutoff = _get_max_nd_extracted_date_for_table(engine, DEIDENTIFIED_SCHEMA, table_name)
                    if cutoff is None:
                        cutoff = date.min
                    stats["cutoff_date"] = cutoff.isoformat()
                    insert_sql = (
                        f"INSERT INTO {diff_fqn} "
                        f"SELECT * FROM {hist_fqn} WHERE nd_extracted_date > :cutoff"
                    )
                    result = conn.execute(text(insert_sql), {"cutoff": cutoff})
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
    if tables_to_copy is not None:
        summary["tables_to_copy"] = tables_to_process

    return summary


def update_diff_schema_history_and_drop_old(
    diff_schema: str,
    history_path: Optional[str] = None,
    keep_last_n: int = 3,
) -> dict:
    """
    Append the current run (diff_schema and diff_schema_deid) to the history file,
    keep only the last keep_last_n runs, then drop any diff_* MySQL schemas that
    are not in the retained set. Keeps diff data for only the last N runs.

    Args:
        diff_schema: e.g. diff_20260226 (the schema name created this run).
        history_path: Path to JSON file. If None, uses airflow_home/diff_schema_history.json.
        keep_last_n: Number of runs to retain (default 3).

    Returns:
        Summary dict with history_path, retained_schemas, dropped_schemas, etc.
    """
    history_path = history_path or _default_history_path()
    deid_schema = f"{diff_schema}_deid"

    # Read current history
    try:
        with open(history_path, "r") as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = []
    if not isinstance(history, list):
        history = []

    # Prepend this run (newest first), keep last keep_last_n
    entry = {"diff_schema": diff_schema, "deid_schema": deid_schema}
    history = [entry] + [h for h in history if isinstance(h, dict) and h.get("diff_schema") != diff_schema]
    history = history[:keep_last_n]

    # Write back
    os.makedirs(os.path.dirname(history_path) or ".", exist_ok=True)
    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)

    retained = set()
    for h in history:
        if isinstance(h, dict):
            retained.add(h.get("diff_schema"))
            retained.add(h.get("deid_schema"))
    retained.discard(None)

    # List all diff_* schemas in MySQL and drop those not retained
    connection_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    engine = create_engine(connection_str, pool_pre_ping=True)
    dropped = []
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'diff_%'")
            ).fetchall()
        schema_names = [r[0] for r in row]
        for name in schema_names:
            if name not in retained:
                try:
                    with engine.connect() as conn:
                        conn.execute(text(f"DROP DATABASE IF EXISTS {_q(name)}"))
                        conn.commit()
                    dropped.append(name)
                except Exception:
                    pass
    finally:
        engine.dispose()

    return {
        "history_path": history_path,
        "retained_schemas": list(retained),
        "dropped_schemas": dropped,
        "kept_last_n": keep_last_n,
    }
