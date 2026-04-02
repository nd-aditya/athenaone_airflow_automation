"""
QC service: compare diff schema vs diff_deid schema row counts per table.
Mirrors the logic of the manual QC script, driven by config values.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text

QC_MAX_WORKERS = 5

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    HISTORICAL_SCHEMA,
    CLIENT_NAME,
    BRIDGE_TABLE_SCHEMA,
)

MAPPING_SCHEMA = "mapping_prod"
MAPPING_TABLE  = "patient_mapping_table"
DOCUMENT_TABLE = "DOCUMENT"

# ---------------------------------------------------------------------------
# Per-table patient-identifier spec.
# Tables NOT listed here fall back to auto-detection
# (patientID → chartID → documentID column scan).
#
# Supported forms (pick one set of keys per entry):
#
#   Direct column on the table itself:
#     {"col": "CHARTID"}
#
#   Custom mapping table join (most common):
#     {"join_col": "clinicalencounterid",
#      "mapping_table": "encounter_mapping_table", "mapping_col": "encounter_id"}
#     Optional: add "mapping_schema": "other_schema" to override MAPPING_SCHEMA
#
#   Single reference hop — join one historical table then check patient col:
#     {"ref_table": "APPOINTMENT_2", "join_col": "APPOINTMENTID", "ref_col": "patientID"}
#
#   Reference chain — multiple LEFT JOIN hops, final col checked against mapping:
#     {"chain": [("CLINICALRESULT", "CLINICALRESULTID"), ("DOCUMENT", "DOCUMENTID")],
#      "col": "patientID"}
# ---------------------------------------------------------------------------
TABLE_IDENTIFIER_MAP = {
    "CLINICALSERVICE":           {"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "APPOINTMENTNOTE":           {"join_col": "APPOINTMENTID",         "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_APPOINTMENTNOTE",             "mapping_col": "APPOINTMENTID"},
    "CLINICALRESULTOBSERVATION": {"join_col": "CLINICALRESULTID",      "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_clinicalresultobservation",    "mapping_col": "CLINICALRESULTID"},
    "ALLERGY":                   {"col": "CHARTID"},
    "APPOINTMENT":               {"join_col": "PATIENT_ID",            "mapping_table": "patient_mapping_table",               "mapping_col": "patientid"},
    "APPOINTMENTVIEW":           {"col": "PATIENTID"},
    "CHART":                     {"col": "CHARTID"},
    "CHARTQUESTIONNAIRE":        {"col": "CHARTID"},
    "CLINICALENCOUNTER":         {"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "CLINICALENCOUNTERDATA":     {"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "CLINICALENCOUNTERDIAGNOSIS":{"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "CLINICALENCOUNTERPREPNOTE": {"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "CLINICALPRESCRIPTION":      {"join_col": "DOCUMENTID",            "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_clinicalprescription",        "mapping_col": "DOCUMENTID"},
    "CLINICALRESULT":            {"join_col": "DOCUMENTID",            "mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_clinicalresult",              "mapping_col": "DOCUMENTID"},
    "CLINICALTEMPLATE":          {"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "DOCUMENT":                  {"col": "CHARTID"},
    "PATIENTMEDICATION":         {"col": "CHARTID"},
    "PATIENTPASTMEDICALHISTORY": {"col": "CHARTID"},
    "PATIENTSURGERY":            {"col": "CHARTID"},
    "PATIENTSURGICALHISTORY":    {"col": "CHARTID"},
    "PATIENTSOCIALHISTORY":      {"col": "CHARTID"},
    "SOCIALHXFORMRESPONSE":      {"col": "CHARTID"},
    "SOCIALHXFORMRESPONSEANSWER":{"join_col": "SOCIALHXFORMRESPONSEID","mapping_schema": BRIDGE_TABLE_SCHEMA, "mapping_table": "bridge_table_socialhxformresponseanswer",  "mapping_col": "SOCIALHXFORMRESPONSEID"},
    "VISIT":                     {"col": "PATIENTID"},
    "VITALSIGN":                 {"join_col": "clinicalencounterid",   "mapping_table": "encounter_mapping_table",             "mapping_col": "encounter_id"},
    "VITALATTRIBUTEREADING":     {"col": "CHARTID"},
    "PATIENTPROBLEM":            {"col": "CHARTID"},
    "PATIENTSNOMEDPROBLEM":      {"col": "CHARTID"},
    "PATIENTSNOMEDICD10":        {"col": "CHARTID"},
}


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


def _count_from_spec(engine, schema: str, table: str, spec: dict) -> tuple:
    """Build and run the ignore-row count query for a given spec."""
    ms = spec.get("mapping_schema", MAPPING_SCHEMA)

    # ── Custom / bridge mapping table ────────────────────────────────────────
    if "mapping_table" in spec:
        join_col      = spec["join_col"]
        mapping_table = spec["mapping_table"]
        mapping_col   = spec["mapping_col"]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(*) FROM `{schema}`.`{table}` t
                LEFT JOIN `{ms}`.`{mapping_table}` m ON m.`{mapping_col}` = t.`{join_col}`
                WHERE m.`{mapping_col}` IS NULL
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    # ── Reference chain (multi-hop LEFT JOINs) ───────────────────────────────
    if "chain" in spec:
        chain, col = spec["chain"], spec["col"]
        aliases = [f"r{i}" for i in range(len(chain))]
        joins, prev_alias = "", "t"
        for i, (join_table, join_col) in enumerate(chain):
            alias = aliases[i]
            joins += (
                f"\nLEFT JOIN `{HISTORICAL_SCHEMA}`.`{join_table}` {alias} "
                f"ON {prev_alias}.`{join_col}` = {alias}.`{join_col}`"
            )
            prev_alias = alias
        last_alias = aliases[-1]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t{joins}
                LEFT JOIN `{ms}`.`{MAPPING_TABLE}` m ON m.`{col}` = {last_alias}.`{col}`
                WHERE {last_alias}.`{col}` IS NOT NULL
                  AND m.`{col}` IS NULL
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    # ── Single reference hop ─────────────────────────────────────────────────
    if "ref_table" in spec:
        ref_table = spec["ref_table"]
        join_col  = spec["join_col"]
        ref_col   = spec["ref_col"]
        with engine.connect() as conn:
            count = conn.execute(text(f"""
                SELECT COUNT(DISTINCT t.nd_auto_increment_id)
                FROM `{schema}`.`{table}` t
                LEFT JOIN `{HISTORICAL_SCHEMA}`.`{ref_table}` r ON t.`{join_col}` = r.`{join_col}`
                LEFT JOIN `{ms}`.`{MAPPING_TABLE}` m ON m.`{ref_col}` = r.`{ref_col}`
                WHERE r.`{ref_col}` IS NOT NULL
                  AND m.`{ref_col}` IS NULL
            """)).scalar()
        return count, "Patient Identifier missing at reference table"

    # ── Direct column ────────────────────────────────────────────────────────
    col = spec["col"]
    if not _col_has_non_null(engine, schema, table, col):
        return None, "Patient Identifier missing at source table"
    with engine.connect() as conn:
        count = conn.execute(text(f"""
            SELECT COUNT(*) FROM `{schema}`.`{table}` t
            LEFT JOIN `{ms}`.`{MAPPING_TABLE}` m ON m.`{col}` = t.`{col}`
            WHERE m.`{col}` IS NULL
        """)).scalar()
    return count, "Patient Identifier missing at source table"


def _ignore_row_count(engine, schema: str, table: str) -> tuple:
    """
    Map-first ignore-row count.
    Checks TABLE_IDENTIFIER_MAP first; falls back to column auto-detection
    for tables not listed in the map.
    """
    spec = TABLE_IDENTIFIER_MAP.get(table.upper())
    if spec is not None:
        return _count_from_spec(engine, schema, table, spec)

    # ── Auto-detection fallback ───────────────────────────────────────────────
    for col in ("patientID", "chartID"):
        if _col_exists(engine, schema, table, col):
            if not _col_has_non_null(engine, schema, table, col):
                return None, "Patient Identifier missing at source table"
            return _count_from_spec(engine, schema, table, {"col": col})

    if _col_exists(engine, schema, table, "documentID"):
        if not _col_has_non_null(engine, schema, table, "documentID"):
            return None, "Patient Identifier missing at source table"
        return _count_from_spec(engine, schema, table, {
            "ref_table": DOCUMENT_TABLE,
            "join_col":  "documentID",
            "ref_col":   "patientID",
        })

    return None, "No patient identifier column in this table"


def build_qc_report(rows: list, errors: list, diff_schema: str, deid_schema: str) -> dict:
    """
    Build the HTML report and result dict from pre-computed rows.
    rows must use keys: table, orig_count, deid_count, diff, ignore_rows, status, comment.
    """
    report_rows  = [r for r in rows if r["orig_count"] > 0]
    pass_count   = sum(1 for r in report_rows if r["status"] == "PASS")
    fail_count   = sum(1 for r in report_rows if r["status"] == "NEED_TO_CHECK")
    failed_count = sum(1 for r in report_rows if r["status"] == "FAILED")

    table_rows_html = ""
    for r in report_rows:
        color       = "#d4edda" if r["status"] == "PASS" else "#f8d7da" if r["status"] == "FAILED" else "#fff3cd"
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

<table cellpadding="0" cellspacing="0" border="0" style="margin-bottom:20px;">
  <tr>
    <td style="padding-right:12px;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr><td style="background:#d4edda;border-radius:6px;padding:12px 24px;font-size:15px;white-space:nowrap;">
          &#9989; <strong>{pass_count}</strong> PASS
        </td></tr>
      </table>
    </td>
    <td style="padding-right:12px;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr><td style="background:#fff3cd;border-radius:6px;padding:12px 24px;font-size:15px;white-space:nowrap;">
          &#9888; <strong>{fail_count}</strong> NEED TO CHECK
        </td></tr>
      </table>
    </td>
    <td style="padding-right:12px;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr><td style="background:#f8d7da;border-radius:6px;padding:12px 24px;font-size:15px;white-space:nowrap;">
          &#10060; <strong>{failed_count}</strong> FAILED
        </td></tr>
      </table>
    </td>
    <td>
      <table cellpadding="0" cellspacing="0" border="0">
        <tr><td style="background:#f8d7da;border-radius:6px;padding:12px 24px;font-size:15px;white-space:nowrap;">
          &#128308; <strong>{len(errors)}</strong> Errors
        </td></tr>
      </table>
    </td>
  </tr>
</table>

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
        "report":       report_html,
        "rows":         rows,
        "errors":       errors,
        "pass_count":   pass_count,
        "fail_count":   fail_count,
        "failed_count": failed_count,
        "diff_schema":  diff_schema,
        "deid_schema":  deid_schema,
    }


def run_qc(diff_schema: str, deid_schema: str) -> dict:
    """
    Compare diff_schema vs deid_schema row counts for every table.

    Returns:
        report      : HTML string ready for email body
        rows        : per-table list of dicts
        errors      : tables that raised exceptions
        pass_count  : tables with status PASS
        fail_count  : tables with status NEED_TO_CHECK
        failed_count: tables with status FAILED
        diff_schema : as passed in
        deid_schema : as passed in
    """
    orig_engine = _engine(diff_schema)
    deid_engine = _engine(deid_schema)

    tables = _all_tables(orig_engine, diff_schema)

    # Case-insensitive lookup: uppercase name → actual stored name
    deid_tables_map = {t.upper(): t for t in _all_tables(deid_engine, deid_schema)}

    rows   = []
    errors = []

    def _process_table(table):
        with orig_engine.connect() as conn:
            orig_count = conn.execute(
                text(f"SELECT COUNT(*) FROM `{diff_schema}`.`{table}`")
            ).scalar() or 0

        deid_table_actual = deid_tables_map.get(table.upper())
        if deid_table_actual is None:
            return {"table": table, "orig_count": orig_count, "deid_count": 0,
                    "diff": orig_count, "ignore_rows": None,
                    "status": "FAILED", "comment": "Table not deidentified"}

        with deid_engine.connect() as conn:
            deid_count = conn.execute(
                text(f"SELECT COUNT(*) FROM `{deid_schema}`.`{deid_table_actual}`")
            ).scalar() or 0

        ignore_rows, comment = _ignore_row_count(orig_engine, diff_schema, table)
        diff       = orig_count - deid_count
        ignore_val = ignore_rows or 0
        status     = "PASS" if abs(diff) - ignore_val == 0 else "NEED_TO_CHECK"
        return {"table": table, "orig_count": orig_count, "deid_count": deid_count,
                "diff": diff, "ignore_rows": ignore_rows,
                "status": status, "comment": comment if diff != 0 else ""}

    with ThreadPoolExecutor(max_workers=QC_MAX_WORKERS) as executor:
        future_to_table = {executor.submit(_process_table, t): t for t in tables}
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                rows.append(future.result())
            except Exception as e:
                errors.append({"table": table, "error": str(e)})

    orig_engine.dispose()
    deid_engine.dispose()

    return build_qc_report(rows, errors, diff_schema, deid_schema)


_TABLE_STYLE = (
    "width:100%;border-collapse:collapse;border:1px solid #dee2e6;"
    "font-size:13px;"
)
