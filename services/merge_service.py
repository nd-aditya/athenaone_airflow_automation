"""
Merge data from incremental schema (dump_daily) to historical schema (athenaone).

Flow (run in order after add_nd_extracted_date):
1. set_historical_flags_to_n: UPDATE historical SET nd_active_flag = 'N' where PK exists in incremental (join).
2. merge_incremental_to_historical: INSERT from incremental into historical (nd_active_flag not in insert list
   so new rows get table default 'Y').
3. validate_historical_one_active_per_pk: Check each primary key has at most one row with nd_active_flag = 'Y'.

Uses table_primary_keys.csv for PK definitions.
"""
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    INCREMENTAL_SCHEMA,
    HISTORICAL_SCHEMA,
)

MAX_WORKERS = 10
ROW_COUNTS = True
ERROR_POLICY_CONTINUE = True
RETRY_ATTEMPTS = 2
RETRY_BACKOFF_SECONDS = 3

TABLE_PRIMARY_KEYS_CSV = os.path.join(
    os.path.dirname(__file__), "..", "table_primary_keys.csv"
)


def _q(name: str) -> str:
    return f"`{name}`"


def _load_primary_keys_csv() -> dict:
    """Load table_primary_keys.csv into {table_name: [pk_col1, pk_col2, ...]}."""
    out = {}
    if not os.path.isfile(TABLE_PRIMARY_KEYS_CSV):
        return out
    with open(TABLE_PRIMARY_KEYS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tname = (row.get("table_name") or "").strip()
            pk_raw = (row.get("primary_key") or "").strip()
            if tname and pk_raw:
                out[tname] = [c.strip() for c in pk_raw.split("|") if c.strip()]
    return out


def _get_columns(engine, schema: str, table: str) -> list:
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table, schema=schema)]


def set_historical_flags_to_n() -> dict:
    """
    For each table present in both HISTORICAL and INCREMENTAL with a PK in table_primary_keys.csv:
    ensure nd_active_flag column exists (ADD if missing with DEFAULT 'N'), then
    UPDATE historical SET nd_active_flag = 'N' WHERE (pk) matches incremental (INNER JOIN).
    Run this before merge_incremental_to_historical.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(connection_str, pool_pre_ping=True)
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    hist_tables = set(inspector.get_table_names(schema=HISTORICAL_SCHEMA))
    incr_tables = set(inspector.get_table_names(schema=INCREMENTAL_SCHEMA))
    common_tables = sorted(hist_tables & incr_tables)

    results = []
    start_all = time.time()

    for table_name in common_tables:
        pk_cols = pk_map.get(table_name) or pk_map.get(table_name.upper())
        if not pk_cols:
            results.append({
                "table": table_name,
                "status": "SKIPPED",
                "error": "No primary key in CSV",
            })
            continue

        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        incr_cols = _get_columns(engine, INCREMENTAL_SCHEMA, table_name)
        if not all(pc in hist_cols for pc in pk_cols):
            results.append({
                "table": table_name,
                "status": "SKIPPED",
                "error": "PK columns not all in historical table",
            })
            continue
        if not all(pc in incr_cols for pc in pk_cols):
            results.append({
                "table": table_name,
                "status": "SKIPPED",
                "error": "PK columns not all in incremental table",
            })
            continue

        hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
        incr_fqn = f"{_q(INCREMENTAL_SCHEMA)}.{_q(table_name)}"
        try:
            with engine.begin() as conn:
                if "nd_active_flag" not in hist_cols:
                    conn.execute(
                        text(f"ALTER TABLE {hist_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'N'")
                    )
                set_n_sql = f"""
                    UPDATE {hist_fqn} h
                    INNER JOIN {incr_fqn} i ON {" AND ".join(f"h.{_q(c)} = i.{_q(c)}" for c in pk_cols)}
                    SET h.`nd_active_flag` = 'N'
                """
                conn.execute(text(set_n_sql))
            results.append({"table": table_name, "status": "SUCCESS"})
        except SQLAlchemyError as e:
            results.append({
                "table": table_name,
                "status": "FAILED",
                "error": f"{type(e).__name__}: {str(e)}",
            })

    total_time = round(time.time() - start_all, 2)
    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    skipped = sum(1 for r in results if r.get("status") == "SKIPPED")
    failed = sum(1 for r in results if r.get("status") == "FAILED")
    engine.dispose()
    return {
        "total_tables": len(results),
        "success": success,
        "skipped": skipped,
        "failed": failed,
        "total_time_seconds": total_time,
        "per_table_results": results,
    }


def _process_table(table_name: str, engine, incr_schema: str, hist_schema: str, pk_map: dict) -> dict:
    stats = {
        "table": table_name,
        "created": False,
        "src_count": None,
        "dst_before": None,
        "dst_after": None,
        "inserted": None,
        "duration": None,
        "error": None,
        "attempts": 0,
    }

    src_fqn = f"{_q(incr_schema)}.{_q(table_name)}"
    dst_fqn = f"{_q(hist_schema)}.{_q(table_name)}"

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        stats["attempts"] = attempt
        start = time.time()
        try:
            with engine.begin() as conn:
                exists_sql = """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :table
                """
                exists = (
                    conn.execute(
                        text(exists_sql),
                        {"schema": hist_schema, "table": table_name},
                    ).scalar()
                    > 0
                )
                if not exists:
                    create_sql = f"CREATE TABLE {dst_fqn} LIKE {src_fqn};"
                    conn.execute(text(create_sql))
                    stats["created"] = True
                    conn.execute(
                        text(f"ALTER TABLE {dst_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'Y'")
                    )

                incr_cols = _get_columns(engine, incr_schema, table_name)
                hist_cols = _get_columns(engine, hist_schema, table_name)
                if "nd_auto_increment_id" in hist_cols:
                    hist_cols = [c for c in hist_cols if c != "nd_auto_increment_id"]
                common_cols = sorted(list(set(incr_cols).intersection(hist_cols)))
                if "nd_active_flag" in common_cols:
                    common_cols = [c for c in common_cols if c != "nd_active_flag"]
                if not common_cols:
                    stats["error"] = "No matching columns"
                    return stats

                if not stats["created"]:
                    next_ai = conn.execute(
                        text(f"SELECT COALESCE(MAX(`nd_auto_increment_id`), 0) + 1 FROM {dst_fqn}")
                    ).scalar()
                    conn.execute(text(f"ALTER TABLE {dst_fqn} AUTO_INCREMENT = :v"), {"v": next_ai})

                quoted_cols = [_q(c) for c in common_cols]
                col_list_str = ", ".join(quoted_cols)

                if ROW_COUNTS:
                    stats["src_count"] = conn.execute(
                        text(f"SELECT COUNT(*) FROM {src_fqn}")
                    ).scalar()
                    stats["dst_before"] = conn.execute(
                        text(f"SELECT COUNT(*) FROM {dst_fqn}")
                    ).scalar()

                insert_sql = (
                    f"INSERT INTO {dst_fqn} ({col_list_str}) "
                    f"SELECT {col_list_str} FROM {src_fqn}"
                )
                conn.execute(text(insert_sql))

                if ROW_COUNTS:
                    stats["dst_after"] = conn.execute(
                        text(f"SELECT COUNT(*) FROM {dst_fqn}")
                    ).scalar()
                    stats["inserted"] = (stats["dst_after"] or 0) - (
                        stats["dst_before"] or 0
                    )

                stats["duration"] = round(time.time() - start, 3)
                return stats

        except SQLAlchemyError as e:
            stats["error"] = f"{type(e).__name__}: {str(e)}"
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_SECONDS)
                continue
            if not ERROR_POLICY_CONTINUE:
                raise
            return stats

    return stats


def merge_incremental_to_historical() -> dict:
    """
    Create missing tables in historical (CREATE TABLE hist LIKE incr; add nd_active_flag DEFAULT 'Y' if new),
    then INSERT INTO hist (common_cols) SELECT common_cols FROM incr.
    nd_active_flag is not included in the insert so new rows get the table default 'Y'.
    Run after set_historical_flags_to_n.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(connection_str, pool_pre_ping=True)
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    incr_tables = inspector.get_table_names(schema=INCREMENTAL_SCHEMA)
    hist_tables = set(inspector.get_table_names(schema=HISTORICAL_SCHEMA))

    results = []
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(
                _process_table,
                tbl,
                engine,
                INCREMENTAL_SCHEMA,
                HISTORICAL_SCHEMA,
                pk_map,
            ): tbl
            for tbl in incr_tables
        }
        for future in as_completed(future_to_table):
            tbl = future_to_table[future]
            try:
                res = future.result()
                results.append(res)
            except Exception as exc:
                results.append(
                    {
                        "table": tbl,
                        "created": False,
                        "src_count": None,
                        "dst_before": None,
                        "dst_after": None,
                        "inserted": None,
                        "duration": None,
                        "error": str(exc),
                        "attempts": 0,
                    }
                )

    total_time = round(time.time() - start_all, 2)
    total_tables = len(results)
    created_count = sum(1 for r in results if r.get("created"))
    succeeded = sum(1 for r in results if not r.get("error"))
    failed = total_tables - succeeded
    total_inserted = sum((r.get("inserted") or 0) for r in results)
    failed_details = [r for r in results if r.get("error")]

    engine.dispose()
    return {
        "total_tables": total_tables,
        "tables_created": created_count,
        "succeeded": succeeded,
        "failed": failed,
        "total_rows_inserted": total_inserted,
        "total_time_seconds": total_time,
        "failed_tables": [{"table": r["table"], "error": r["error"]} for r in failed_details],
        "per_table_results": results,
    }


def _validate_one_active_per_pk(conn, hist_fqn: str, pk_cols: list) -> int:
    """Return count of PKs that have more than one row with nd_active_flag = 'Y'."""
    pk_part = ", ".join(_q(c) for c in pk_cols)
    bad_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT {pk_part}
            FROM {hist_fqn}
            WHERE `nd_active_flag` = 'Y'
            GROUP BY {pk_part}
            HAVING COUNT(*) > 1
        ) t
    """
    return conn.execute(text(bad_sql)).scalar() or 0


def validate_historical_one_active_per_pk(merge_summary: Optional[dict] = None) -> dict:
    """
    For each table (only those with rows inserted when merge_summary provided, else all in both schemas),
    check that each primary key has at most one row with nd_active_flag = 'Y'.
    Run after merge_incremental_to_historical.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(connection_str, pool_pre_ping=True)
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    all_hist_tables = inspector.get_table_names(schema=HISTORICAL_SCHEMA)
    incr_tables_set = set(inspector.get_table_names(schema=INCREMENTAL_SCHEMA))

    if merge_summary and merge_summary.get("per_table_results"):
        tables_with_inserts = {
            r["table"]
            for r in merge_summary["per_table_results"]
            if (r.get("inserted") or 0) > 0
        }
        tables_to_validate = [t for t in all_hist_tables if t in tables_with_inserts and t in incr_tables_set]
    else:
        tables_to_validate = [t for t in all_hist_tables if t in incr_tables_set]

    results = []
    validation_failed_tables = []

    for table_name in tables_to_validate:
        pk_cols = pk_map.get(table_name) or pk_map.get(table_name.upper())
        if not pk_cols:
            results.append({"table": table_name, "status": "SKIPPED", "error": "No primary key in CSV"})
            continue

        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        if "nd_active_flag" not in hist_cols or not all(pc in hist_cols for pc in pk_cols):
            results.append({"table": table_name, "status": "SKIPPED", "error": "Missing columns"})
            continue

        hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
        try:
            with engine.connect() as conn:
                bad_count = _validate_one_active_per_pk(conn, hist_fqn, pk_cols)
            if bad_count > 0:
                validation_failed_tables.append({"table": table_name, "duplicate_active_pks": bad_count})
                results.append({"table": table_name, "status": "FAILED", "duplicate_active_pks": bad_count})
            else:
                results.append({"table": table_name, "status": "SUCCESS"})
        except SQLAlchemyError as e:
            results.append({"table": table_name, "status": "FAILED", "error": str(e)})
            validation_failed_tables.append({"table": table_name, "error": str(e)})

    engine.dispose()
    return {
        "total_tables": len(results),
        "success": sum(1 for r in results if r.get("status") == "SUCCESS"),
        "skipped": sum(1 for r in results if r.get("status") == "SKIPPED"),
        "failed": sum(1 for r in results if r.get("status") == "FAILED"),
        "validation_failed_tables": validation_failed_tables,
        "per_table_results": results,
    }
