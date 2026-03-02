"""
Merge data from incremental schema (dump_daily) to historical schema (athenaone).
Creates missing tables in historical, then INSERT INTO ... SELECT for common columns.
Handles reserved keywords by quoting column names; nd_auto_increment_id remains NULL for new rows.
Merge task is insert-only. A separate step (ensure_historical_indexes_and_update_flags) creates
indexes (idx_<table>_pk and idx_<table>_norm) if missing and sets nd_active_flag (one row per PK = 'Y').
Uses table_primary_keys.csv for PK definitions.
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
TEXT_PREFIX_LEN = 255


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


def _build_index_columns(conn, schema: str, table: str, cols: list) -> list:
    """Return list of index column specs (TEXT/BLOB get prefix length)."""
    rows = conn.execute(
        text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t AND column_name IN :c
        """),
        {"s": schema, "t": table, "c": tuple(cols)},
    ).fetchall()
    text_types = {"text", "blob", "mediumtext", "longtext"}
    text_cols = {r[0] for r in rows if (r[1] or "").lower() in text_types}
    return [
        f"`{c}`({TEXT_PREFIX_LEN})" if c in text_cols else f"`{c}`"
        for c in cols
    ]


def _index_exists(conn, schema: str, table: str, idx: str) -> bool:
    return conn.execute(
        text("""
            SELECT 1 FROM information_schema.statistics
            WHERE table_schema = :s AND table_name = :t AND index_name = :i
        """),
        {"s": schema, "t": table, "i": idx},
    ).scalar() is not None


def _create_index(conn, schema: str, table: str, idx: str, col_specs: list) -> None:
    conn.execute(text(f"""
        CREATE INDEX `{idx}` ON `{schema}`.`{table}` ({", ".join(col_specs)})
    """))


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
    Sync all tables from INCREMENTAL_SCHEMA to HISTORICAL_SCHEMA (insert only).
    Creates missing tables (CREATE TABLE hist.t LIKE incr.t), then
    INSERT INTO hist (common_cols) SELECT common_cols FROM incr.
    Does not create indexes or update nd_active_flag; use ensure_historical_indexes_and_update_flags for that.

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


def ensure_historical_indexes_and_update_flags() -> dict:
    """
    For each table in HISTORICAL_SCHEMA that has a primary key in table_primary_keys.csv:
    ensures idx_<table>_pk and idx_<table>_norm exist (creates if missing), then sets
    nd_active_flag so one row per PK is 'Y' (latest by LASTUPDATED/nd_auto_increment_id) and rest 'N'.
    Intended to run as a separate DAG task after merge_incremental_to_historical (insert-only).

    Returns a summary dict for XCom: total_tables, indexes_created, flags_updated, failed, per_table results.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(connection_str, pool_pre_ping=True)
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    hist_tables = inspector.get_table_names(schema=HISTORICAL_SCHEMA)

    results = []
    start_all = time.time()

    for table_name in hist_tables:
        pk_cols = pk_map.get(table_name) or pk_map.get(table_name.upper())
        if not pk_cols:
            results.append({
                "table": table_name,
                "status": "SKIPPED",
                "indexes_created": [],
                "error": "No primary key in CSV",
            })
            continue

        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        if not all(pc in hist_cols for pc in pk_cols):
            results.append({
                "table": table_name,
                "status": "SKIPPED",
                "indexes_created": [],
                "error": "PK columns not all in table",
            })
            continue

        hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
        indexes_created = []
        try:
            with engine.begin() as conn:
                idx_pk = f"idx_{table_name}_pk"
                if not _index_exists(conn, HISTORICAL_SCHEMA, table_name, idx_pk):
                    col_specs = _build_index_columns(conn, HISTORICAL_SCHEMA, table_name, pk_cols)
                    _create_index(conn, HISTORICAL_SCHEMA, table_name, idx_pk, col_specs)
                    indexes_created.append(idx_pk)

                norm_cols = pk_cols + ["LASTUPDATED", "nd_auto_increment_id"]
                if all(c in hist_cols for c in norm_cols):
                    idx_norm = f"idx_{table_name}_norm"
                    if not _index_exists(conn, HISTORICAL_SCHEMA, table_name, idx_norm):
                        col_specs = _build_index_columns(conn, HISTORICAL_SCHEMA, table_name, norm_cols)
                        _create_index(conn, HISTORICAL_SCHEMA, table_name, idx_norm, col_specs)
                        indexes_created.append(idx_norm)

                _update_nd_active_flag(conn, table_name, hist_fqn, hist_cols, pk_map)
            results.append({
                "table": table_name,
                "status": "SUCCESS",
                "indexes_created": indexes_created,
            })
        except SQLAlchemyError as e:
            results.append({
                "table": table_name,
                "status": "FAILED",
                "indexes_created": indexes_created,
                "error": f"{type(e).__name__}: {str(e)}",
            })

    total_time = round(time.time() - start_all, 2)
    total_tables = len(results)
    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    skipped = sum(1 for r in results if r.get("status") == "SKIPPED")
    failed = sum(1 for r in results if r.get("status") == "FAILED")
    indexes_created_count = sum(len(r.get("indexes_created") or []) for r in results)

    summary = {
        "total_tables": total_tables,
        "success": success,
        "skipped": skipped,
        "failed": failed,
        "indexes_created_count": indexes_created_count,
        "total_time_seconds": total_time,
        "per_table_results": results,
    }
    engine.dispose()
    return summary
