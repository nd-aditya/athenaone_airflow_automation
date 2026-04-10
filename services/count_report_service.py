"""
Priority-table count report.

Connects to:
  • MAC  — localhost MySQL, DEIDENTIFIED_SCHEMA (deidentified_merged)
  • GCP  — remote restore-machine MySQL, one schema per provider machine
            (configured via GCP_REPORT_SCHEMAS in services/config.py)

For every priority table it fetches:
  COUNT(*)                              → total rows
  COUNT(*) WHERE nd_active_flag = 'Y'  → active rows

CLI usage:
    python services/count_report_service.py
    python services/count_report_service.py --csv report.csv
    python services/count_report_service.py --mac-only
    python services/count_report_service.py --gcp-only
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime

from sqlalchemy import create_engine, text

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    DEIDENTIFIED_SCHEMA,
    GCP_MYSQL_HOST,
    GCP_MYSQL_USER,
    GCP_MYSQL_PASSWORD,
    GCP_MYSQL_PORT,
    GCP_REPORT_SCHEMAS,
)

# Priority tables — matches TEST_TABLE_NAMES in extraction_dag.py
PRIORITY_TABLES = [
    "ALLERGY",
    "APPOINTMENT",
    "APPOINTMENTELIGIBILITYINFO",
    "APPOINTMENTNOTE",
    "APPOINTMENTVIEW",
    "CHART",
    "CHARTQUESTIONNAIRE",
    "CHARTQUESTIONNAIREANSWER",
    "CLINICALENCOUNTER",
    "CLINICALENCOUNTERDATA",
    "CLINICALENCOUNTERDIAGNOSIS",
    "CLINICALENCOUNTERDXICD10",
    "CLINICALENCOUNTERPREPNOTE",
    "CLINICALORDERTYPE",
    "clinicalprescription",
    "CLINICALRESULT",
    "CLINICALRESULTOBSERVATION",
    "CLINICALSERVICE",
    "CLINICALSERVICEPROCEDURECODE",
    "CLINICALTEMPLATE",
    "document",
    "FDB_RMIID1",
    "FDB_RNDC14",
    "ICDCODEALL",
    "INSURANCEPACKAGE",
    "medication",
    "PATIENT",
    "PATIENTALLERGY",
    "PATIENTALLERGYREACTION",
    "PATIENTFAMILYHISTORY",
    "PATIENTINSURANCE",
    "patientmedication",
    "PATIENTPASTMEDICALHISTORY",
    "PATIENTSOCIALHISTORY",
    "PATIENTSURGERY",
    "PATIENTSURGICALHISTORY",
    "PROCEDURECODE",
    "PROCEDURECODEREFERENCE",
    "SNOMED",
    "SOCIALHXFORMRESPONSE",
    "SOCIALHXFORMRESPONSEANSWER",
    "SURGICALHISTORYPROCEDURE",
    "visit",
    "VITALATTRIBUTEREADING",
    "VITALSIGN",
    "PROVIDER",
    "PROVIDERGROUP",
    "PATIENTPROBLEM",
    "PATIENTSNOMEDPROBLEM",
    "PATIENTSNOMEDICD10",
    "PATIENTGPALHISTORY",
]


def _count_table(conn, schema: str, table: str) -> tuple[int, int]:
    """Return (total_count, active_y_count). Returns (-1, -1) on error (table missing/permission)."""
    try:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
        ).scalar() or 0
        active = conn.execute(
            text(f"SELECT COUNT(*) FROM `{schema}`.`{table}` WHERE nd_active_flag = 'Y'")
        ).scalar() or 0
        return int(total), int(active)
    except Exception:
        return -1, -1


def generate_count_report(
    tables: list[str] | None = None,
    include_mac: bool = True,
    include_gcp: bool = True,
) -> list[dict]:
    """
    Query MAC and/or GCP for each priority table.

    Returns a list of dicts, one per table:
      {
        "table": str,
        "mac_total": int,        # -1 if table missing / error
        "mac_active_y": int,
        "gcp_<LABEL>_total": int,   # one pair per GCP_REPORT_SCHEMAS entry
        "gcp_<LABEL>_active_y": int,
      }
    """
    tables = tables or PRIORITY_TABLES

    mac_engine = None
    gcp_engine = None

    if include_mac:
        mac_engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/",
            pool_pre_ping=True,
        )
    if include_gcp:
        if not GCP_MYSQL_HOST:
            raise ValueError(
                "GCP_MYSQL_HOST is not set in services/config.py. "
                "Please add the GCP restore machine's IP/hostname."
            )
        gcp_engine = create_engine(
            f"mysql+pymysql://{GCP_MYSQL_USER}:{GCP_MYSQL_PASSWORD}"
            f"@{GCP_MYSQL_HOST}:{GCP_MYSQL_PORT}/",
            pool_pre_ping=True,
        )

    rows: list[dict] = []

    try:
        mac_conn = mac_engine.connect() if mac_engine else None
        gcp_conn = gcp_engine.connect() if gcp_engine else None

        try:
            for table in tables:
                row: dict = {"table": table}

                if mac_conn is not None:
                    total, active = _count_table(mac_conn, DEIDENTIFIED_SCHEMA, table)
                    row["mac_total"]    = total
                    row["mac_active_y"] = active

                if gcp_conn is not None:
                    for label, schema in GCP_REPORT_SCHEMAS.items():
                        total, active = _count_table(gcp_conn, schema, table)
                        row[f"gcp_{label}_total"]    = total
                        row[f"gcp_{label}_active_y"] = active

                rows.append(row)
        finally:
            if mac_conn:
                mac_conn.close()
            if gcp_conn:
                gcp_conn.close()
    finally:
        if mac_engine:
            mac_engine.dispose()
        if gcp_engine:
            gcp_engine.dispose()

    return rows


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _fmt(n: int) -> str:
    return "N/A" if n == -1 else f"{n:,}"


def print_report(rows: list[dict], include_mac: bool = True, include_gcp: bool = True) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    gcp_labels = list(GCP_REPORT_SCHEMAS.keys())

    # Build header
    headers = ["Table"]
    if include_mac:
        headers += [f"MAC Total ({DEIDENTIFIED_SCHEMA})", "MAC Active(Y)"]
    if include_gcp:
        for label in gcp_labels:
            headers += [f"GCP {label} Total", f"GCP {label} Active(Y)"]

    # Column widths
    col_w = [max(len(h), 32) for h in headers]
    col_w[0] = max(max(len(r["table"]) for r in rows), len(headers[0]))

    sep = "-+-".join("-" * w for w in col_w)
    header_line = " | ".join(h.ljust(col_w[i]) for i, h in enumerate(headers))

    print(f"\nPriority Table Count Report  —  {ts}")
    print("=" * len(header_line))
    print(header_line)
    print(sep)

    for row in rows:
        cells = [row["table"].ljust(col_w[0])]
        i = 1
        if include_mac:
            cells.append(_fmt(row.get("mac_total", -1)).ljust(col_w[i]))
            i += 1
            cells.append(_fmt(row.get("mac_active_y", -1)).ljust(col_w[i]))
            i += 1
        if include_gcp:
            for label in gcp_labels:
                cells.append(_fmt(row.get(f"gcp_{label}_total", -1)).ljust(col_w[i]))
                i += 1
                cells.append(_fmt(row.get(f"gcp_{label}_active_y", -1)).ljust(col_w[i]))
                i += 1
        print(" | ".join(cells))

    print(sep)
    print(f"  {len(rows)} tables\n")


def export_csv(rows: list[dict], path: str) -> None:
    if not rows:
        print("No data to export.")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"CSV saved → {path}")


# ── CLI entry point ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Priority-table count report (MAC vs GCP)")
    parser.add_argument("--csv", metavar="FILE", help="Also export results to a CSV file")
    parser.add_argument("--mac-only", action="store_true", help="Query MAC only (skip GCP)")
    parser.add_argument("--gcp-only", action="store_true", help="Query GCP only (skip MAC)")
    args = parser.parse_args()

    include_mac = not args.gcp_only
    include_gcp = not args.mac_only

    print("Fetching counts…")
    try:
        rows = generate_count_report(include_mac=include_mac, include_gcp=include_gcp)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    print_report(rows, include_mac=include_mac, include_gcp=include_gcp)

    if args.csv:
        export_csv(rows, args.csv)


if __name__ == "__main__":
    main()
