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
from concurrent.futures import ThreadPoolExecutor, as_completed
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


COPY_TO_DIFF_MAX_WORKERS = 10
IDX_ND_EXTRACTED_DATE = "idx_nd_extracted_date"


def _nd_extracted_date_index_exists(conn, schema: str, table_name: str) -> bool:
    """Return True if the table has an index whose first column is nd_extracted_date."""
    row = conn.execute(
        text("""
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = :s AND table_name = :t
              AND column_name = 'nd_extracted_date' AND seq_in_index = 1
            LIMIT 1
        """),
        {"s": schema, "t": table_name},
    ).fetchone()
    return row is not None


def _ensure_nd_extracted_date_index(conn, schema: str, table_name: str) -> None:
    """Create index on nd_extracted_date if it does not exist (speeds up copy WHERE nd_extracted_date > cutoff)."""
    if _nd_extracted_date_index_exists(conn, schema, table_name):
        return
    fqn = f"{_q(schema)}.{_q(table_name)}"
    conn.execute(text(f"ALTER TABLE {fqn} ADD INDEX `{IDX_ND_EXTRACTED_DATE}` (`nd_extracted_date`)"))


def _copy_one_table_to_diff(table_name: str, engine, diff_schema: str) -> dict:
    """
    Copy one table from historical to diff schema. Creates diff table like historical,
    then INSERT based on nd_extracted_date cutoff from DEIDENTIFIED_SCHEMA. Returns stats dict.
    """
    stats = {"table": table_name, "inserted": 0, "duration": None, "error": None}
    start = time.time()
    try:
        inspector = inspect(engine)
        hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
        diff_fqn = f"{_q(diff_schema)}.{_q(table_name)}"
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE {diff_fqn} LIKE {hist_fqn}"))
            columns = [c["name"] for c in inspector.get_columns(table_name, schema=HISTORICAL_SCHEMA)]
            has_nd_extracted_date = "nd_extracted_date" in [c.lower() for c in columns]
            if has_nd_extracted_date:
                _ensure_nd_extracted_date_index(conn, HISTORICAL_SCHEMA, table_name)
                cutoff = _get_max_nd_extracted_date_for_table(engine, DEIDENTIFIED_SCHEMA, table_name)
                if cutoff is None:
                    cutoff = date.min
                stats["cutoff_date"] = cutoff.isoformat()
                result = conn.execute(
                    text(f"INSERT INTO {diff_fqn} SELECT * FROM {hist_fqn} WHERE nd_extracted_date > :cutoff"),
                    {"cutoff": cutoff},
                )
            else:
                result = conn.execute(text(f"INSERT INTO {diff_fqn} SELECT * FROM {hist_fqn}"))
            stats["inserted"] = result.rowcount if result.rowcount is not None else 0
        stats["duration"] = round(time.time() - start, 3)
        return stats
    except SQLAlchemyError as e:
        stats["error"] = f"{type(e).__name__}: {str(e)}"
        stats["duration"] = round(time.time() - start, 3)
        return stats


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
    engine = create_engine(
        connection_str,
        pool_pre_ping=True,
        pool_size=COPY_TO_DIFF_MAX_WORKERS,
        max_overflow=2,
    )
    inspector = inspect(engine)

    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {_q(diff_schema)}"))
        conn.commit()

    hist_tables_all = inspector.get_table_names(schema=HISTORICAL_SCHEMA)
    # Case-insensitive: include every historical table whose name (any case) was requested.
    # Build from hist_tables_all so we use actual DB table names and never miss due to case.
    if tables_to_copy is not None:
        requested_lower = {t.lower() for t in tables_to_copy}
        tables_to_process = [t for t in hist_tables_all if t.lower() in requested_lower]
    else:
        tables_to_process = [
            t for t in hist_tables_all
            if "nd_extracted_date" in [c["name"].lower() for c in inspector.get_columns(t, schema=HISTORICAL_SCHEMA)]
        ]

    results = []
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=COPY_TO_DIFF_MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(_copy_one_table_to_diff, table_name, engine, diff_schema): table_name
            for table_name in tables_to_process
        }
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                stats = future.result()
                results.append(stats)
            except Exception as e:
                results.append({
                    "table": table_name,
                    "inserted": 0,
                    "duration": None,
                    "error": str(e),
                })

    total_time = round(time.time() - start_all, 2)
    engine.dispose()
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
