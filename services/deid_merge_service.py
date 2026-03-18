"""
Merge deidentified data from diff_<date>_deid schema into DEIDENTIFIED_SCHEMA (config).
Optimized flow (like merge_service):
1. set_deid_merged_flags_to_n: UPDATE merged SET nd_active_flag = 'N' only for rows whose PK exists in deid_schema.
2. INSERT from deid_schema into merged with nd_active_flag = 'Y' (new rows).
3. validate_deid_merged_one_active_per_pk: check one active row per PK per table.
4. fix_deid_merged_one_active_per_pk: for tables that failed validation, SET all to N then one per PK to Y.
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
    DEIDENTIFIED_SCHEMA,
)

TABLE_PRIMARY_KEYS_CSV = os.path.join(
    os.path.dirname(__file__), "..", "table_primary_keys.csv"
)
TEXT_PREFIX_LEN = 255
MERGE_DEID_MAX_WORKERS = 10
SET_DEID_FLAGS_MAX_WORKERS = 8
VALIDATION_DEID_MAX_WORKERS = 8
FIX_DEID_MAX_WORKERS = 8
TEMP_DEID_PK_PREFIX = "tmp_deid_pks_"


def _q(name: str) -> str:
    return f"`{name}`"


def _norm(t: str) -> str:
    return t.lower()


def _load_pk_config() -> dict:
    """Return {normalized_table_name: [pk_col1, pk_col2, ...]}."""
    out = {}
    if not os.path.isfile(TABLE_PRIMARY_KEYS_CSV):
        return out
    with open(TABLE_PRIMARY_KEYS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            tname = (row.get("table_name") or "").strip()
            pk_raw = (row.get("primary_key") or "").strip()
            if tname and pk_raw:
                out[_norm(tname)] = [c.strip() for c in pk_raw.split("|") if c.strip()]
    return out


def _get_table_map(engine, incr_schema: str) -> tuple[dict, dict]:
    """
    Return (common_map, incr_only).
    common_map: {logical_name: (hist_table_name, incr_table_name)} for tables in both schemas.
    incr_only: {logical_name: incr_table_name} for tables in incr_schema but not in DEIDENTIFIED_SCHEMA.
    """
    insp = inspect(engine)
    hist = {_norm(t): t for t in insp.get_table_names(schema=DEIDENTIFIED_SCHEMA)}
    incr = {_norm(t): t for t in insp.get_table_names(schema=incr_schema)}
    common = set(hist) & set(incr)
    incr_only = {t: incr[t] for t in sorted(set(incr) - set(hist))}
    common_map = {t: (hist[t], incr[t]) for t in sorted(common)}
    return common_map, incr_only


def _create_table_from_deid(engine, incr_schema: str, table_name: str) -> None:
    """Create table in DEIDENTIFIED_SCHEMA like incr_schema.table_name and add nd_active_flag if missing."""
    hist_fqn = f"{_q(DEIDENTIFIED_SCHEMA)}.{_q(table_name)}"
    incr_fqn = f"{_q(incr_schema)}.{_q(table_name)}"
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {hist_fqn} LIKE {incr_fqn}"))
        cols = [c["name"] for c in inspect(engine).get_columns(table_name, schema=DEIDENTIFIED_SCHEMA)]
        if "nd_active_flag" not in [c.lower() for c in cols]:
            conn.execute(text(f"ALTER TABLE {hist_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'Y'"))


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


def _deid_pk_table_name(table_name: str) -> str:
    """Temp table name for deid PK staging (MySQL identifier max 64 chars)."""
    name = f"{TEMP_DEID_PK_PREFIX}{table_name}"
    return name[:64] if len(name) > 64 else name


def _set_deid_merged_flags_to_n_one_table(
    engine,
    hist_tbl: str,
    incr_tbl: str,
    pk_cols: list,
    deid_schema: str,
) -> dict:
    """
    For one table: create temp table of PKs from deid_schema, UPDATE merged SET nd_active_flag = 'N'
    only where PK in that temp, then drop temp. Returns result dict.
    """
    hist_fqn = f"{_q(DEIDENTIFIED_SCHEMA)}.{_q(hist_tbl)}"
    incr_fqn = f"{_q(deid_schema)}.{_q(incr_tbl)}"
    try:
        with engine.connect() as conn:
            incr_count = conn.execute(text(f"SELECT COUNT(*) FROM {incr_fqn}")).scalar() or 0
        if incr_count == 0:
            return {"table": hist_tbl, "status": "SKIPPED", "logs": ["deid has 0 rows"]}
    except SQLAlchemyError as e:
        return {"table": hist_tbl, "status": "FAILED", "logs": [f"count check: {e}"]}

    pk_select = ", ".join(_q(c) for c in pk_cols)
    pk_join = " AND ".join(f"h.{_q(c)} = t.{_q(c)}" for c in pk_cols)
    pk_table_name = _deid_pk_table_name(hist_tbl)
    pk_table_fqn = f"{_q(DEIDENTIFIED_SCHEMA)}.{_q(pk_table_name)}"
    try:
        with engine.begin() as conn:
            hist_cols = [c["name"] for c in inspect(engine).get_columns(hist_tbl, schema=DEIDENTIFIED_SCHEMA)]
            if "nd_active_flag" not in [c.lower() for c in hist_cols]:
                conn.execute(text(f"ALTER TABLE {hist_fqn} ADD COLUMN `nd_active_flag` VARCHAR(1) DEFAULT 'Y'"))
            conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
            conn.execute(text(f"CREATE TABLE {pk_table_fqn} AS SELECT {pk_select} FROM {incr_fqn}"))
            idx_col_specs = _build_index_columns(conn, DEIDENTIFIED_SCHEMA, pk_table_name, pk_cols)
            if idx_col_specs:
                conn.execute(text(f"CREATE INDEX idx_pk ON {pk_table_fqn} ({', '.join(idx_col_specs)})"))
            conn.execute(text(f"""
                UPDATE {hist_fqn} h
                INNER JOIN {pk_table_fqn} t ON {pk_join}
                SET h.`nd_active_flag` = 'N'
            """))
            conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
        return {"table": hist_tbl, "status": "SUCCESS", "logs": []}
    except SQLAlchemyError as e:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {pk_table_fqn}"))
        except SQLAlchemyError:
            pass
        return {"table": hist_tbl, "status": "FAILED", "logs": [str(e)]}


def set_deid_merged_flags_to_n(
    engine,
    deid_schema: str,
    common_tasks: list[tuple[str, str, list]],
) -> list[dict]:
    """
    For each (hist_tbl, incr_tbl, pk_cols) in common_tasks: set nd_active_flag = 'N' in merged
    only for rows whose PK exists in deid_schema. Runs in parallel. Returns per_table results.
    """
    results = []
    with ThreadPoolExecutor(max_workers=SET_DEID_FLAGS_MAX_WORKERS) as executor:
        future_to_hist = {
            executor.submit(
                _set_deid_merged_flags_to_n_one_table,
                engine,
                hist_tbl,
                incr_tbl,
                pk_cols,
                deid_schema,
            ): hist_tbl
            for (hist_tbl, incr_tbl, pk_cols) in common_tasks
        }
        for future in as_completed(future_to_hist):
            hist_tbl = future_to_hist[future]
            try:
                results.append(future.result())
            except Exception as e:
                results.append({"table": hist_tbl, "status": "FAILED", "logs": [str(e)]})
    return results


def _merge_table(
    engine,
    hist_tbl: str,
    incr_tbl: str,
    pk_cols: list,
    incr_schema: str,
) -> dict:
    """Insert from deid_schema into merged with nd_active_flag = 'Y'. No full-table N or normalize here."""
    start = time.time()
    logs = []
    inserted = 0
    try:
        with engine.begin() as conn:
            idx_pk = f"idx_{hist_tbl}_pk"
            if not _index_exists(conn, DEIDENTIFIED_SCHEMA, hist_tbl, idx_pk):
                cols = _build_index_columns(conn, DEIDENTIFIED_SCHEMA, hist_tbl, pk_cols)
                _create_index(conn, DEIDENTIFIED_SCHEMA, hist_tbl, idx_pk, cols)
                logs.append(f"Created PK index {idx_pk}")

            norm_cols = pk_cols + ["LASTUPDATED", "nd_auto_increment_id"]
            idx_norm = f"idx_{hist_tbl}_norm"
            if not _index_exists(conn, DEIDENTIFIED_SCHEMA, hist_tbl, idx_norm):
                cols = _build_index_columns(conn, DEIDENTIFIED_SCHEMA, hist_tbl, norm_cols)
                _create_index(conn, DEIDENTIFIED_SCHEMA, hist_tbl, idx_norm, cols)
                logs.append(f"Created norm index {idx_norm}")

            hist_fqn = f"{_q(DEIDENTIFIED_SCHEMA)}.{_q(hist_tbl)}"
            incr_fqn = f"{_q(incr_schema)}.{_q(incr_tbl)}"

            cols = [
                c["name"]
                for c in inspect(engine).get_columns(hist_tbl, schema=DEIDENTIFIED_SCHEMA)
                if c["name"] != "nd_active_flag"
            ]
            col_sql = ", ".join(_q(c) for c in cols)
            result = conn.execute(text(f"""
                INSERT INTO {hist_fqn} ({col_sql}, `nd_active_flag`)
                SELECT {col_sql}, 'Y' FROM {incr_fqn}
            """))
            inserted = result.rowcount if result.rowcount is not None else 0
            logs.append(f"Inserted {inserted} rows")

        status = "SUCCESS"
    except Exception as e:
        logs.append(f"ERROR: {e}")
        status = "FAILED"

    return {
        "table": hist_tbl,
        "status": status,
        "duration": round(time.time() - start, 2),
        "logs": logs,
        "inserted": inserted,
    }


def _validate_deid_table_one_active_per_pk(
    engine,
    table_name: str,
    pk_cols: list,
) -> dict:
    """Validate one table: pass if COUNT(DISTINCT pk) == count of rows with nd_active_flag = 'Y'."""
    hist_fqn = f"{_q(DEIDENTIFIED_SCHEMA)}.{_q(table_name)}"
    distinct_part = ", ".join(_q(c) for c in pk_cols)
    sql = f"""
        SELECT
            COUNT(DISTINCT {distinct_part}) AS distinct_pk,
            SUM(CASE WHEN `nd_active_flag` = 'Y' THEN 1 ELSE 0 END) AS active_count
        FROM {hist_fqn}
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql)).fetchone()
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
        return {"table": table_name, "status": "FAILED", "reason": str(e)}


def validate_deid_merged_one_active_per_pk(
    engine,
    merge_results: list[dict],
    pk_map: dict,
) -> dict:
    """
    For each table in merge_results that succeeded (and had inserts or is merged), validate
    one active row per PK. Returns summary with validation_failed_tables.
    """
    tables_to_validate = [
        r["table"]
        for r in merge_results
        if r.get("status") == "SUCCESS"
        and (r.get("inserted", 0) > 0 or r.get("inserted") is None)
    ]
    results = []
    validation_failed_tables = []
    with ThreadPoolExecutor(max_workers=VALIDATION_DEID_MAX_WORKERS) as executor:
        future_to_table = {}
        for table_name in tables_to_validate:
            pk_cols = pk_map.get(_norm(table_name))
            if not pk_cols:
                results.append({"table": table_name, "status": "SKIPPED", "reason": "No PK in CSV"})
                continue
            future_to_table[
                executor.submit(
                    _validate_deid_table_one_active_per_pk,
                    engine,
                    table_name,
                    pk_cols,
                )
            ] = table_name
        for future in as_completed(future_to_table):
            try:
                res = future.result()
                results.append(res)
                if res.get("status") == "FAILED":
                    validation_failed_tables.append({
                        "table": res["table"],
                        "reason": res.get("reason", ""),
                        "active_row_count": res.get("active_row_count"),
                        "distinct_pk_count": res.get("distinct_pk_count"),
                    })
            except Exception as e:
                table_name = future_to_table[future]
                results.append({"table": table_name, "status": "FAILED", "reason": str(e)})
                validation_failed_tables.append({"table": table_name, "reason": str(e)})
    return {
        "per_table_results": results,
        "validation_failed_tables": validation_failed_tables,
        "success": sum(1 for r in results if r.get("status") == "SUCCESS"),
        "failed": sum(1 for r in results if r.get("status") == "FAILED"),
        "skipped": sum(1 for r in results if r.get("status") == "SKIPPED"),
    }


def _fix_deid_merged_one_table(
    engine,
    table_name: str,
    pk_cols: list,
) -> dict:
    """Fix one table: SET nd_active_flag = 'N' for all, then SET 'Y' for one row per PK (latest)."""
    hist_fqn = f"{_q(DEIDENTIFIED_SCHEMA)}.{_q(table_name)}"
    pk_part = ", ".join(_q(c) for c in pk_cols)
    try:
        hist_cols = [c["name"] for c in inspect(engine).get_columns(table_name, schema=DEIDENTIFIED_SCHEMA)]
        if "LASTUPDATED" in [c.upper() for c in hist_cols]:
            order_clause = "ORDER BY `LASTUPDATED` DESC, `nd_auto_increment_id` DESC"
        else:
            order_clause = "ORDER BY `nd_auto_increment_id` DESC"
        with engine.begin() as conn:
            conn.execute(text(f"UPDATE {hist_fqn} SET `nd_active_flag` = 'N'"))
            conn.execute(text(f"""
                UPDATE {hist_fqn} h
                INNER JOIN (
                    SELECT nd_auto_increment_id
                    FROM (
                        SELECT
                            nd_auto_increment_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY {pk_part}
                                {order_clause}
                            ) rn
                        FROM {hist_fqn}
                    ) t WHERE rn = 1
                ) x ON h.nd_auto_increment_id = x.nd_auto_increment_id
                SET h.`nd_active_flag` = 'Y'
            """))
        return {"table": table_name, "status": "SUCCESS"}
    except SQLAlchemyError as e:
        return {"table": table_name, "status": "FAILED", "reason": str(e)}


def fix_deid_merged_one_active_per_pk(
    engine,
    validation_summary: Optional[dict] = None,
) -> dict:
    """
    For each table in validation_summary["validation_failed_tables"], fix nd_active_flag:
    SET all to 'N', then SET 'Y' for one row per PK (latest by LASTUPDATED/nd_auto_increment_id).
    """
    if not validation_summary:
        return {"fixed_tables": 0, "skipped": 0, "failed": 0, "per_table": []}
    failed_list = validation_summary.get("validation_failed_tables") or []
    if not failed_list:
        return {"fixed_tables": 0, "skipped": 0, "failed": 0, "per_table": []}

    pk_map = _load_pk_config()
    per_table = []
    fixed = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=FIX_DEID_MAX_WORKERS) as executor:
        future_to_entry = {}
        for entry in failed_list:
            table_name = entry.get("table")
            if not table_name:
                continue
            pk_cols = pk_map.get(_norm(table_name))
            if not pk_cols:
                per_table.append({"table": table_name, "status": "SKIPPED", "reason": "No PK in CSV"})
                continue
            future_to_entry[
                executor.submit(_fix_deid_merged_one_table, engine, table_name, pk_cols)
            ] = table_name
        for future in as_completed(future_to_entry):
            try:
                res = future.result()
                per_table.append(res)
                if res.get("status") == "SUCCESS":
                    fixed += 1
                else:
                    failed += 1
            except Exception as e:
                table_name = future_to_entry[future]
                per_table.append({"table": table_name, "status": "FAILED", "reason": str(e)})
                failed += 1
    return {"fixed_tables": fixed, "skipped": len(failed_list) - fixed - failed, "failed": failed, "per_table": per_table}


def merge_deid_to_merged(deid_schema: str) -> dict:
    """
    Merge tables from deid_schema (e.g. diff_20260225_deid) into DEIDENTIFIED_SCHEMA.
    Optimized flow: set existing (matched on PK) to N → insert new as Y → validate → fix failed tables.
    Returns summary for XCom.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(
        connection_str,
        pool_pre_ping=True,
        pool_size=max(MERGE_DEID_MAX_WORKERS, SET_DEID_FLAGS_MAX_WORKERS, VALIDATION_DEID_MAX_WORKERS, FIX_DEID_MAX_WORKERS),
        max_overflow=2,
    )

    pk_map = _load_pk_config()
    table_map, incr_only = _get_table_map(engine, deid_schema)
    results = []
    merge_tasks = []
    common_tasks = []

    # Build common_tasks (for set N) and merge_tasks
    for logical, (hist_tbl, incr_tbl) in table_map.items():
        if logical not in pk_map:
            results.append({
                "table": hist_tbl,
                "status": "SKIPPED",
                "duration": 0,
                "logs": ["No primary key in CSV"],
            })
            continue
        common_tasks.append((hist_tbl, incr_tbl, pk_map[logical]))
        merge_tasks.append((hist_tbl, incr_tbl, pk_map[logical]))

    # Tables only in deid schema: create in DEIDENTIFIED_SCHEMA then add to merge_tasks (no set N)
    for logical, incr_tbl in incr_only.items():
        if logical not in pk_map:
            results.append({
                "table": incr_tbl,
                "status": "SKIPPED",
                "duration": 0,
                "logs": ["No primary key in CSV"],
            })
            continue
        try:
            _create_table_from_deid(engine, deid_schema, incr_tbl)
            merge_tasks.append((incr_tbl, incr_tbl, pk_map[logical]))
        except Exception as e:
            results.append({
                "table": incr_tbl,
                "status": "FAILED",
                "duration": 0,
                "logs": [f"Create table failed: {e}"],
            })

    # 1. Set existing rows (PK in deid_schema) to N
    set_flags_results = set_deid_merged_flags_to_n(engine, deid_schema, common_tasks)

    # 2. Insert from deid_schema with nd_active_flag = 'Y'
    with ThreadPoolExecutor(max_workers=MERGE_DEID_MAX_WORKERS) as executor:
        future_to_hist = {
            executor.submit(
                _merge_table,
                engine,
                hist_tbl,
                incr_tbl,
                pk_cols,
                deid_schema,
            ): hist_tbl
            for (hist_tbl, incr_tbl, pk_cols) in merge_tasks
        }
        for future in as_completed(future_to_hist):
            hist_tbl = future_to_hist[future]
            try:
                results.append(future.result())
            except Exception as e:
                results.append({
                    "table": hist_tbl,
                    "status": "FAILED",
                    "duration": 0,
                    "logs": [f"ERROR: {e}"],
                    "inserted": 0,
                })

    results.sort(key=lambda r: r.get("table", ""))

    # 3. Validate one active per PK for all merged tables
    validation_summary = validate_deid_merged_one_active_per_pk(engine, results, pk_map)

    # 4. Fix tables that failed validation (full table: all N then one Y per PK)
    fix_summary = fix_deid_merged_one_active_per_pk(engine, validation_summary)

    engine.dispose()

    total = len(results)
    success = sum(1 for r in results if r["status"] == "SUCCESS")
    skipped = sum(1 for r in results if r["status"] == "SKIPPED")
    failed = sum(1 for r in results if r["status"] == "FAILED")

    return {
        "deid_schema": deid_schema,
        "target_schema": DEIDENTIFIED_SCHEMA,
        "total_tables": total,
        "success": success,
        "skipped": skipped,
        "failed": failed,
        "per_table": results,
        "set_flags_per_table": set_flags_results,
        "validation": validation_summary,
        "fix": fix_summary,
    }
