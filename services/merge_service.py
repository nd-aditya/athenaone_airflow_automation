"""
Merge data from incremental schema (dump_daily) to historical schema (athenaone).
Creates missing tables in historical, then INSERT INTO ... SELECT for common columns.
Handles reserved keywords by quoting column names; nd_auto_increment_id remains NULL for new rows.
After insert, updates nd_active_flag so one row per primary key is 'Y' (latest by LASTUPDATED)
and all others 'N'. Uses table_primary_keys.csv for PK definitions.
"""
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    INCREMENTAL_SCHEMA,
    HISTORICAL_SCHEMA,
)

# Tuning
MAX_WORKERS = 10
ROW_COUNTS = True
ERROR_POLICY_CONTINUE = True
RETRY_ATTEMPTS = 2
RETRY_BACKOFF_SECONDS = 3

TABLE_PRIMARY_KEYS_CSV = os.path.join(
    os.path.dirname(__file__), "..", "table_primary_keys.csv"
)


def _q(name: str) -> str:
    """Quote a column or table name with backticks."""
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


def _update_nd_active_flag(conn, table_name: str, hist_fqn: str, hist_cols: list, pk_map: dict) -> None:
    """
    Set nd_active_flag: one row per primary key = 'Y' (latest by LASTUPDATED),
    all others = 'N'. Skips if table or CSV lacks required columns.
    """
    if "nd_active_flag" not in hist_cols:
        conn.execute(
            text(f"ALTER TABLE {hist_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'N'")
        )
        hist_cols = list(hist_cols) + ["nd_active_flag"]
    if "nd_auto_increment_id" not in hist_cols or "LASTUPDATED" not in hist_cols:
        return
    pk_cols = pk_map.get(table_name) or pk_map.get(table_name.upper())
    if not pk_cols or not all(pc in hist_cols for pc in pk_cols):
        return
    pk_part = ", ".join(_q(c) for c in pk_cols)
    conn.execute(text(f"UPDATE {hist_fqn} SET `nd_active_flag` = 'N'"))
    update_sql = f"""
        UPDATE {hist_fqn} h
        INNER JOIN (
            SELECT nd_auto_increment_id
            FROM (
                SELECT
                    nd_auto_increment_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY {pk_part}
                        ORDER BY `LASTUPDATED` DESC, `nd_auto_increment_id` DESC
                    ) rn
                FROM {hist_fqn}
            ) t
            WHERE rn = 1
        ) x ON h.nd_auto_increment_id = x.nd_auto_increment_id
        SET h.`nd_active_flag` = 'Y'
    """
    conn.execute(text(update_sql))


def _get_columns(engine, schema: str, table: str) -> list:
    inspector = inspect(engine)
    return [col["name"] for col in inspector.get_columns(table, schema=schema)]


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

                incr_cols = _get_columns(engine, incr_schema, table_name)
                hist_cols = _get_columns(engine, hist_schema, table_name)
                has_nd_auto_increment = "nd_auto_increment_id" in hist_cols
                if has_nd_auto_increment:
                    hist_cols.remove("nd_auto_increment_id")

                common_cols = sorted(list(set(incr_cols).intersection(hist_cols)))
                if not common_cols:
                    stats["error"] = "No matching columns"
                    return stats

                # So next INSERT gets nd_auto_increment_id = max(existing) + 1 (fixes gap after deletes)
                if has_nd_auto_increment and not stats["created"]:
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

                hist_cols_full = _get_columns(engine, hist_schema, table_name)
                try:
                    _update_nd_active_flag(conn, table_name, dst_fqn, hist_cols_full, pk_map)
                except SQLAlchemyError:
                    pass

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
    Sync all tables from INCREMENTAL_SCHEMA to HISTORICAL_SCHEMA.
    Creates missing tables (CREATE TABLE hist.t LIKE incr.t), then
    INSERT INTO hist (common_cols) SELECT common_cols FROM incr.
    Then sets nd_active_flag (one row per PK = 'Y', rest 'N') using table_primary_keys.csv.

    Returns a summary dict for XCom: total_tables, created, succeeded, failed,
    total_inserted, total_time_seconds, per_table results.
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

    summary = {
        "total_tables": total_tables,
        "tables_created": created_count,
        "succeeded": succeeded,
        "failed": failed,
        "total_rows_inserted": total_inserted,
        "total_time_seconds": total_time,
        "failed_tables": [{"table": r["table"], "error": r["error"]} for r in failed_details],
        "per_table_results": results,
    }

    return summary
