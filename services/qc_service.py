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
    CLIENT_NAME,
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

    deid_tables = set(_all_tables(deid_engine, deid_schema))

    for table in tables:
        try:
            with orig_engine.connect() as conn:
                orig_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{diff_schema}`.`{table}`")
                ).scalar() or 0

            if table not in deid_tables:
                rows.append({
                    "table": table,
                    "orig_count": orig_count,
                    "deid_count": 0,
                    "diff": orig_count,
                    "ignore_rows": None,
                    "status": "FAILED",
                    "comment": "Table not deidentified",
                })
                continue

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
                "comment": comment if diff != 0 else "",
            })
        except Exception as e:
            errors.append({"table": table, "error": str(e)})

    orig_engine.dispose()
    deid_engine.dispose()

    report_rows = [r for r in rows if r["orig_count"] > 0]

    pass_count = sum(1 for r in report_rows if r["status"] == "PASS")
    fail_count = sum(1 for r in report_rows if r["status"] == "NEED_TO_CHECK")

    table_rows_html = ""
    for r in report_rows:
        color = "#d4edda" if r["status"] == "PASS" else "#f8d7da" if r["status"] == "FAILED" else "#fff3cd"
        badge_color = "#28a745" if r["status"] == "PASS" else "#c0392b" if r["status"] == "FAILED" else "#e67e22"
        ignore_display = "N/A" if r["ignore_rows"] is None else f"{r['ignore_rows']:,}"
        table_rows_html += f"""
        <tr style="background:{color};">
            <td>{r['table']}</td>
            <td style="text-align:right;">{r['orig_count']:,}</td>
            <td style="text-align:right;">{r['deid_count']:,}</td>
            <td style="text-align:right;">{r['diff']:,}</td>
            <td style="text-align:right;">{ignore_display}</td>
            <td style="text-align:center;">
                <span style="background:{badge_color};color:#fff;padding:2px 8px;border-radius:4px;font-weight:bold;">
                    {r['status']}
                </span>
            </td>
            <td style="color:#555;">{r['comment']}</td>
        </tr>"""

    errors_html = ""
    if errors:
        error_rows = "".join(
            f"<tr><td>{e['table']}</td><td style='color:red;'>{e['error']}</td></tr>"
            for e in errors
        )
        errors_html = f"""
        <h3 style="color:#c0392b;margin-top:32px;">Errors ({len(errors)})</h3>
        <table style="{_TABLE_STYLE}">
            <thead><tr style="background:#c0392b;color:#fff;">
                <th style="text-align:left;padding:8px;">Table</th>
                <th style="text-align:left;padding:8px;">Error</th>
            </tr></thead>
            <tbody>{error_rows}</tbody>
        </table>"""

    report_html = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;font-size:13px;color:#222;max-width:1100px;margin:auto;padding:24px;">
<h2 style="margin-bottom:4px;">{CLIENT_NAME} Deidentification Quantitative QC Report</h2>
<p style="color:#555;margin-top:0;">{diff_schema} &nbsp;vs&nbsp; {deid_schema}</p>

<div style="display:flex;gap:16px;margin-bottom:20px;">
    <div style="background:#d4edda;border-radius:6px;padding:12px 24px;font-size:15px;">
        ✅ <strong>{pass_count}</strong> PASS
    </div>
    <div style="background:#fff3cd;border-radius:6px;padding:12px 24px;font-size:15px;">
        ⚠️ <strong>{fail_count}</strong> NEED TO CHECK
    </div>
    <div style="background:#f8d7da;border-radius:6px;padding:12px 24px;font-size:15px;">
        ❌ <strong>{len(errors)}</strong> Errors
    </div>
</div>

<table style="{_TABLE_STYLE}">
    <thead>
        <tr style="background:#343a40;color:#fff;">
            <th style="text-align:left;padding:8px;">Table</th>
            <th style="text-align:right;padding:8px;">Orig Count</th>
            <th style="text-align:right;padding:8px;">Deid Count</th>
            <th style="text-align:right;padding:8px;">Diff</th>
            <th style="text-align:right;padding:8px;">Ignore Rows</th>
            <th style="text-align:center;padding:8px;">Status</th>
            <th style="text-align:left;padding:8px;">Comments</th>
        </tr>
    </thead>
    <tbody>{table_rows_html}</tbody>
</table>
{errors_html}
</body></html>"""

    return {
        "report": report_html,
        "rows": rows,
        "errors": errors,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "diff_schema": diff_schema,
        "deid_schema": deid_schema,
    }


_TABLE_STYLE = (
    "width:100%;border-collapse:collapse;border:1px solid #dee2e6;"
    "font-size:13px;"
)