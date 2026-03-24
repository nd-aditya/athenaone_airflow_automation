"""
QC service: compare diff schema vs diff_deid schema row counts per table.
Mirrors the logic of the manual QC script, driven by config values.
"""
from sqlalchemy import create_engine, text

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    HISTORICAL_SCHEMA,
)

MAPPING_SCHEMA = "mapping_prod"
MAPPING_TABLE = "patient_mapping_table"
DOCUMENT_TABLE = "DOCUMENT"


def _engine(schema: str):
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{schema}",
        pool_pre_ping=True,
    )


def _all_tables(engine, schema: str) -> list:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :s ORDER BY TABLE_NAME"),
            {"s": schema},
        ).fetchall()
    return [r[0] for r in rows]


def _col_exists(engine, schema: str, table: str, column: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(
            text("""
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t AND COLUMN_NAME = :c LIMIT 1
            """),
            {"s": schema, "t": table, "c": column},
        ).fetchone() is not None


def _col_has_non_null(engine, schema: str, table: str, column: str) -> bool:
    with engine.connect() as conn:
        return conn.execute(
            text(f"SELECT 1 FROM `{schema}`.`{table}` WHERE `{column}` IS NOT NULL LIMIT 1")
        ).fetchone() is not None


def _ignore_row_count(engine, schema: str, table: str) -> tuple:
    """Return (ignore_count, comment). ignore_count is None when no patient identifier applies."""
    tl = table.lower()

    if tl == "clinicalservice":
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(*)
                FROM `{schema}`.`{table}` t
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`encounter_mapping_table` m
                    WHERE m.encounter_id = t.clinicalencounterid
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    if tl == "appointmentnote":
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t
                JOIN `{HISTORICAL_SCHEMA}`.`APPOINTMENT_2` a ON t.APPOINTMENTID = a.APPOINTMENTID
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                    WHERE m.patientID = a.patientID
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    if tl == "clinicalresultobservation":
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t
                JOIN `{HISTORICAL_SCHEMA}`.`CLINICALRESULT` cr ON t.CLINICALRESULTID = cr.CLINICALRESULTID
                JOIN `{HISTORICAL_SCHEMA}`.`{DOCUMENT_TABLE}` d ON cr.DOCUMENTID = d.DOCUMENTID
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                    WHERE m.patientID = d.patientID
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    for col in ("patientID", "chartID"):
        if _col_exists(engine, schema, table, col):
            if not _col_has_non_null(engine, schema, table, col):
                return None, "Patient Identifier missing at source table"
            with engine.connect() as conn:
                count = conn.execute(text(f"""
                    SELECT COUNT(*) FROM `{schema}`.`{table}` t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                        WHERE m.`{col}` = t.`{col}`
                    )
                """)).scalar()
            return count, "Patient Identifier missing at source table"

    if _col_exists(engine, schema, table, "documentID"):
        if not _col_has_non_null(engine, schema, table, "documentID"):
            return None, "Patient Identifier missing at source table"
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t
                JOIN `{HISTORICAL_SCHEMA}`.`{DOCUMENT_TABLE}` d ON t.documentID = d.documentID
                WHERE NOT EXISTS (
                    SELECT 1 FROM `{MAPPING_SCHEMA}`.`{MAPPING_TABLE}` m
                    WHERE m.patientID = d.patientID
                )
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    return None, "No patient identifier column in this table"


def run_qc(diff_schema: str, deid_schema: str) -> dict:
    """
    Compare diff_schema vs deid_schema row counts for every table.

    Returns:
        report      : formatted plain-text string ready for email body
        rows        : per-table list of dicts
        errors      : tables that raised exceptions
        pass_count  : tables with status PASS
        fail_count  : tables with status NEED_TO_CHECK
        diff_schema : as passed in
        deid_schema : as passed in
    """
    orig_engine = _engine(diff_schema)
    deid_engine = _engine(deid_schema)

    tables = _all_tables(orig_engine, diff_schema)
    rows = []
    errors = []

    for table in tables:
        try:
            with orig_engine.connect() as conn:
                orig_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{diff_schema}`.`{table}`")
                ).scalar() or 0
            with deid_engine.connect() as conn:
                deid_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{deid_schema}`.`{table}`")
                ).scalar() or 0
            ignore_rows, comment = _ignore_row_count(orig_engine, diff_schema, table)
            diff = orig_count - deid_count
            ignore_val = ignore_rows or 0
            status = "PASS" if abs(diff) - ignore_val == 0 else "NEED_TO_CHECK"
            rows.append({
                "table": table,
                "orig_count": orig_count,
                "deid_count": deid_count,
                "diff": diff,
                "ignore_rows": ignore_rows,
                "status": status,
                "comment": comment,
            })
        except Exception as e:
            errors.append({"table": table, "error": str(e)})

    orig_engine.dispose()
    deid_engine.dispose()

    pass_count = sum(1 for r in rows if r["status"] == "PASS")
    fail_count = sum(1 for r in rows if r["status"] == "NEED_TO_CHECK")

    lines = [
        f"QC Report: {diff_schema}  vs  {deid_schema}",
        "=" * 142,
        (
            f"{'TABLE':30}{'ORIG_CNT':>12}{'DEID_CNT':>12}{'DIFF':>10}"
            f"{'IGNORE_ROWS':>14}{'STATUS':>14}{'COMMENTS':>50}"
        ),
        "-" * 142,
    ]
    for r in rows:
        lines.append(
            f"{r['table']:30}{r['orig_count']:12}{r['deid_count']:12}{r['diff']:10}"
            f"{'N/A' if r['ignore_rows'] is None else str(r['ignore_rows']):>14}"
            f"{r['status']:>14}{r['comment']:>50}"
        )
    lines += [
        "",
        f"Summary: {pass_count} PASS  |  {fail_count} NEED_TO_CHECK  |  {len(errors)} errors",
    ]
    if errors:
        lines += ["", "ERROR SUMMARY", "=" * 100]
        for e in errors:
            lines.append(f"{e['table']:30} {e['error']}")

    return {
        "report": "\n".join(lines),
        "rows": rows,
        "errors": errors,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "diff_schema": diff_schema,
        "deid_schema": deid_schema,
    }