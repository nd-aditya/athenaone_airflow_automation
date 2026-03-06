"""
Merge deidentified data from diff_<date>_deid schema into DEIDENTIFIED_SCHEMA (config).
Used after deid completes in DAG 2 and DAG 3. Creates PK/normalization indexes if missing,
INSERTs from deid schema into merged, then sets nd_active_flag (one active row per PK).
"""
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, inspect, text

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


def _get_table_map(engine, incr_schema: str) -> dict:
    """Return {logical_name: (hist_table_name, incr_table_name)} for tables in both schemas."""
    insp = inspect(engine)
    hist = {_norm(t): t for t in insp.get_table_names(schema=DEIDENTIFIED_SCHEMA)}
    incr = {_norm(t): t for t in insp.get_table_names(schema=incr_schema)}
    common = set(hist) & set(incr)
    return {t: (hist[t], incr[t]) for t in sorted(common)}


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


def _merge_table(
    engine,
    hist_tbl: str,
    incr_tbl: str,
    pk_cols: list,
    incr_schema: str,
) -> dict:
    start = time.time()
    logs = []
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

            conn.execute(text(f"UPDATE {hist_fqn} SET `nd_active_flag` = 'N'"))

            cols = [
                c["name"]
                for c in inspect(engine).get_columns(hist_tbl, schema=DEIDENTIFIED_SCHEMA)
                if c["name"] != "nd_active_flag"
            ]
            col_sql = ", ".join(_q(c) for c in cols)
            inserted = conn.execute(text(f"""
                INSERT INTO {hist_fqn} ({col_sql}, `nd_active_flag`)
                SELECT {col_sql}, 'N' FROM {incr_fqn}
            """)).rowcount
            logs.append(f"Inserted {inserted} rows")

            pk_part = ", ".join(_q(c) for c in pk_cols)
            conn.execute(text(f"""
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
                    ) t WHERE rn = 1
                ) x ON h.nd_auto_increment_id = x.nd_auto_increment_id
                SET h.`nd_active_flag` = 'Y'
            """))

            bad = conn.execute(text(f"""
                SELECT COUNT(*) FROM (
                    SELECT {pk_part}
                    FROM {hist_fqn}
                    WHERE `nd_active_flag` = 'Y'
                    GROUP BY {pk_part}
                    HAVING COUNT(*) > 1
                ) t
            """)).scalar()
            if bad:
                raise RuntimeError("Multiple active rows detected after normalize")

        status = "SUCCESS"
    except Exception as e:
        logs.append(f"ERROR: {e}")
        status = "FAILED"

    return {
        "table": hist_tbl,
        "status": status,
        "duration": round(time.time() - start, 2),
        "logs": logs,
    }


def merge_deid_to_merged(deid_schema: str) -> dict:
    """
    Merge tables from deid_schema (e.g. diff_20260225_deid) into DEIDENTIFIED_SCHEMA.
    Uses table_primary_keys.csv for PK. Runs table merges in parallel (MERGE_DEID_MAX_WORKERS).
    Returns summary for XCom.
    """
    connection_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    )
    engine = create_engine(
        connection_str,
        pool_pre_ping=True,
        pool_size=MERGE_DEID_MAX_WORKERS,
        max_overflow=2,
    )

    pk_map = _load_pk_config()
    table_map = _get_table_map(engine, deid_schema)
    results = []

    # Skipped (no PK in CSV) — add first
    merge_tasks = []
    for logical, (hist_tbl, incr_tbl) in table_map.items():
        if logical not in pk_map:
            results.append({
                "table": hist_tbl,
                "status": "SKIPPED",
                "duration": 0,
                "logs": ["No primary key in CSV"],
            })
            continue
        merge_tasks.append((hist_tbl, incr_tbl, pk_map[logical]))

    # Run merge in parallel
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
                })

    engine.dispose()

    # Sort per_table by table name for deterministic output
    results.sort(key=lambda r: r.get("table", ""))

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
    }
