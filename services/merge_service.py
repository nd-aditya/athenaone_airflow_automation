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


TEMP_PK_TABLE_PREFIX = "tmp_incr_pks_"
SET_FLAGS_MAX_WORKERS = 8
TEXT_PREFIX_LEN = 255


def _build_index_columns(conn, schema: str, table: str, cols: list) -> list:
    """Return index column specs for CREATE INDEX; TEXT/BLOB get prefix length."""
    if not cols:
        return []
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


def _pk_table_name_for(table_name: str) -> str:
    """Return a unique table name for the PK staging table (MySQL identifier max 64 chars)."""
    name = f"{TEMP_PK_TABLE_PREFIX}{table_name}"
    return name[:64] if len(name) > 64 else name


def _set_historical_flags_to_n_one_table(
    table_name: str,
    engine,
    pk_cols: list,
    hist_cols: list,
    hist_fqn: str,
    incr_fqn: str,
) -> dict:
    """
    For one table: if incremental has rows, create PK staging table, index it, UPDATE historical
    SET nd_active_flag = 'N' via JOIN, then drop staging table. Returns result dict.
    """
    try:
        with engine.connect() as conn:
            incr_count = conn.execute(text(f"SELECT COUNT(*) FROM {incr_fqn}")).scalar() or 0
        if incr_count == 0:
            return {"table": table_name, "status": "SKIPPED", "error": "incremental has 0 rows"}
    except SQLAlchemyError as e:
        return {
            "table": table_name,
            "status": "FAILED",
            "error": f"count check: {type(e).__name__}: {str(e)}",
        }

    pk_select = ", ".join(_q(c) for c in pk_cols)
    pk_join = " AND ".join(f"h.{_q(c)} = t.{_q(c)}" for c in pk_cols)
    pk_table_name = _pk_table_name_for(table_name)
    pk_table_fqn = f"{_q(INCREMENTAL_SCHEMA)}.{_q(pk_table_name)}"
    try:
        with engine.begin() as conn:
            if "nd_active_flag" not in hist_cols:
                conn.execute(
                    text(f"ALTER TABLE {hist_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'Y'")
                )
            conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
            conn.execute(
                text(f"CREATE TABLE {pk_table_fqn} AS SELECT {pk_select} FROM {incr_fqn}")
            )
            idx_col_specs = _build_index_columns(conn, INCREMENTAL_SCHEMA, table_name, pk_cols)
            if idx_col_specs:
                idx_cols_str = ", ".join(idx_col_specs)
                conn.execute(
                    text(f"CREATE INDEX idx_pk ON {pk_table_fqn} ({idx_cols_str})")
                )
            conn.execute(
                text(f"""
                    UPDATE {hist_fqn} h
                    INNER JOIN {pk_table_fqn} t ON {pk_join}
                    SET h.`nd_active_flag` = 'N'
                """)
            )
            conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
        return {"table": table_name, "status": "SUCCESS"}
    except SQLAlchemyError as e:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
        except SQLAlchemyError:
            pass
        return {
            "table": table_name,
            "status": "FAILED",
            "error": f"{type(e).__name__}: {str(e)}",
        }


def set_historical_flags_to_n() -> dict:
    """
    For each table present in both HISTORICAL and INCREMENTAL with a PK in table_primary_keys.csv
    and COUNT(*) > 0 in incremental: DROP TABLE IF EXISTS then CREATE TABLE (in INCREMENTAL_SCHEMA,
    name tmp_incr_pks_<table>) with only PK columns from incremental, add an index, UPDATE historical
    SET nd_active_flag = 'N' via JOIN, then DROP TABLE. Runs in parallel (SET_FLAGS_MAX_WORKERS).
    Run this before merge_incremental_to_historical.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(
        connection_str,
        pool_pre_ping=True,
        pool_size=SET_FLAGS_MAX_WORKERS,
        max_overflow=2,
    )
    inspector = inspect(engine)
    pk_map = _load_primary_keys_csv()
    hist_tables = set(inspector.get_table_names(schema=HISTORICAL_SCHEMA))
    incr_tables = set(inspector.get_table_names(schema=INCREMENTAL_SCHEMA))
    common_tables = sorted(hist_tables & incr_tables)

    skipped_pre = []
    tasks = []
    for table_name in common_tables:
        pk_cols = pk_map.get(table_name) or pk_map.get(table_name.upper())
        if not pk_cols:
            skipped_pre.append({"table": table_name, "status": "SKIPPED", "error": "No primary key in CSV"})
            continue
        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        incr_cols = _get_columns(engine, INCREMENTAL_SCHEMA, table_name)
        if not all(pc in hist_cols for pc in pk_cols):
            skipped_pre.append({"table": table_name, "status": "SKIPPED", "error": "PK columns not all in historical table"})
            continue
        if not all(pc in incr_cols for pc in pk_cols):
            skipped_pre.append({"table": table_name, "status": "SKIPPED", "error": "PK columns not all in incremental table"})
            continue
        hist_fqn = f"{_q(HISTORICAL_SCHEMA)}.{_q(table_name)}"
        incr_fqn = f"{_q(INCREMENTAL_SCHEMA)}.{_q(table_name)}"
        tasks.append((table_name, pk_cols, hist_cols, hist_fqn, incr_fqn))

    results = list(skipped_pre)
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=SET_FLAGS_MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(
                _set_historical_flags_to_n_one_table,
                table_name,
                engine,
                pk_cols,
                hist_cols,
                hist_fqn,
                incr_fqn,
            ): table_name
            for (table_name, pk_cols, hist_cols, hist_fqn, incr_fqn) in tasks
        }
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                res = future.result()
                results.append(res)
            except Exception as e:
                results.append({"table": table_name, "status": "FAILED", "error": str(e)})

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


VALIDATION_MAX_WORKERS = 8


def _validate_table_one_active_per_pk(
    table_name: str,
    engine,
    hist_schema: str,
    pk_cols: list,
) -> dict:
    """
    Validate one table: pass if COUNT(DISTINCT pk_cols) == count of rows with nd_active_flag = 'Y'.
    Uses a single query (one table scan) to get both values.
    """
    hist_fqn = f"{_q(hist_schema)}.{_q(table_name)}"
    distinct_part = ", ".join(_q(c) for c in pk_cols)
    combined_sql = f"""
        SELECT
            COUNT(DISTINCT {distinct_part}) AS distinct_pk,
            SUM(CASE WHEN `nd_active_flag` = 'Y' THEN 1 ELSE 0 END) AS active_count
        FROM {hist_fqn}
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(combined_sql)).fetchone()
        distinct_pk_count = (row[0] or 0) if row else 0
        active_count = (row[1] or 0) if row and len(row) > 1 else 0
        if distinct_pk_count == active_count:
            return {"table": table_name, "status": "SUCCESS"}
        return {
            "table": table_name,
            "status": "FAILED",
            "active_row_count": active_count,
            "distinct_pk_count": distinct_pk_count,
            "reason": f"active_row_count={active_count} != distinct_pk_count={distinct_pk_count}",
        }
    except SQLAlchemyError as e:
        return {"table": table_name, "status": "FAILED", "error": str(e)}


def validate_historical_one_active_per_pk(merge_summary: Optional[dict] = None) -> dict:
    """
    For each table (only those with rows inserted when merge_summary provided, else all in both schemas),
    validate: COUNT(DISTINCT pk_cols) == COUNT(*) WHERE nd_active_flag = 'Y'. If equal, pass.
    Runs validation in parallel (VALIDATION_MAX_WORKERS). Run after merge_incremental_to_historical.
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

    skipped = []
    to_run = []
    for table_name in tables_to_validate:
        pk_cols = pk_map.get(table_name) or pk_map.get(table_name.upper())
        if not pk_cols:
            skipped.append({"table": table_name, "status": "SKIPPED", "error": "No primary key in CSV"})
            continue
        hist_cols = _get_columns(engine, HISTORICAL_SCHEMA, table_name)
        if "nd_active_flag" not in hist_cols or not all(pc in hist_cols for pc in pk_cols):
            skipped.append({"table": table_name, "status": "SKIPPED", "error": "Missing columns"})
            continue
        to_run.append((table_name, pk_cols))

    results = list(skipped)
    validation_failed_tables = []
    start_all = time.time()

    with ThreadPoolExecutor(max_workers=VALIDATION_MAX_WORKERS) as executor:
        future_to_table = {
            executor.submit(
                _validate_table_one_active_per_pk,
                table_name,
                engine,
                HISTORICAL_SCHEMA,
                pk_cols,
            ): table_name
            for table_name, pk_cols in to_run
        }
        for future in as_completed(future_to_table):
            table_name = future_to_table[future]
            try:
                res = future.result()
                results.append(res)
                if res.get("status") == "FAILED":
                    validation_failed_tables.append({
                        "table": res["table"],
                        "reason": res.get("reason") or res.get("error"),
                        "active_row_count": res.get("active_row_count"),
                        "distinct_pk_count": res.get("distinct_pk_count"),
                    })
            except Exception as e:
                results.append({"table": table_name, "status": "FAILED", "error": str(e)})
                validation_failed_tables.append({"table": table_name, "error": str(e)})

    total_time = round(time.time() - start_all, 2)
    engine.dispose()
    return {
        "total_tables": len(results),
        "success": sum(1 for r in results if r.get("status") == "SUCCESS"),
        "skipped": sum(1 for r in results if r.get("status") == "SKIPPED"),
        "failed": sum(1 for r in results if r.get("status") == "FAILED"),
        "total_time_seconds": total_time,
        "validation_failed_tables": validation_failed_tables,
        "per_table_results": results,
    }
