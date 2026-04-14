"""
Run on GCP machine.
Fetches COUNT(*) and COUNT(*) WHERE nd_active_flag='Y' for each priority table
across all provider schemas, then uploads results to GCS.

Usage:
    python count_report_gcp.py
"""
import csv
import io
import pymysql
from google.cloud import storage

# ── Config (keep in sync with services/config.py GCP_REPORT_SCHEMAS) ──────────
HOST     = "localhost"
USER     = "nd-root-mysql"
PASSWORD = "kmsamd89undsd4"

GCS_BUCKET = "nd-platform-dcnd"              # COUNT_REPORT_GCS_BUCKET in services/config.py
GCS_PATH   = "count_reports/gcp_counts.csv"  # COUNT_REPORT_GCP_GCS_PATH in services/config.py

SCHEMAS = {
    "TNG":     "tng_athena_one",
    "DCND":    "dcnd",
    "TNCPA":   "tncpa",
    "Raleigh": "raleigh",
}

TABLES = [
    "ALLERGY", "APPOINTMENT", "APPOINTMENTELIGIBILITYINFO", "APPOINTMENTNOTE",
    "APPOINTMENTVIEW", "CHART", "CHARTQUESTIONNAIRE", "CHARTQUESTIONNAIREANSWER",
    "CLINICALENCOUNTER", "CLINICALENCOUNTERDATA", "CLINICALENCOUNTERDIAGNOSIS",
    "CLINICALENCOUNTERDXICD10", "CLINICALENCOUNTERPREPNOTE", "CLINICALORDERTYPE",
    "clinicalprescription", "CLINICALRESULT", "CLINICALRESULTOBSERVATION",
    "CLINICALSERVICE", "CLINICALSERVICEPROCEDURECODE", "CLINICALTEMPLATE",
    "document", "FDB_RMIID1", "FDB_RNDC14", "ICDCODEALL", "INSURANCEPACKAGE",
    "medication", "PATIENT", "PATIENTALLERGY", "PATIENTALLERGYREACTION",
    "PATIENTFAMILYHISTORY", "PATIENTINSURANCE", "patientmedication",
    "PATIENTPASTMEDICALHISTORY", "PATIENTSOCIALHISTORY", "PATIENTSURGERY",
    "PATIENTSURGICALHISTORY", "PROCEDURECODE", "PROCEDURECODEREFERENCE",
    "SNOMED", "SOCIALHXFORMRESPONSE", "SOCIALHXFORMRESPONSEANSWER",
    "SURGICALHISTORYPROCEDURE", "visit", "VITALATTRIBUTEREADING", "VITALSIGN",
    "PROVIDER", "PROVIDERGROUP", "PATIENTPROBLEM", "PATIENTSNOMEDPROBLEM",
    "PATIENTSNOMEDICD10", "PATIENTGPALHISTORY",
]

# ── Fetch ─────────────────────────────────────────────────────────────────────
print("Connecting to GCP MySQL…")
conn = pymysql.connect(host=HOST, user=USER, password=PASSWORD)
cur  = conn.cursor()

rows = []

for label, schema in SCHEMAS.items():
    print(f"\n{'='*62}")
    print(f"  Schema: {schema}  ({label})")
    print(f"{'='*62}")
    print(f"{'Table':<35} {'Total':>12} {'Active (Y)':>12}")
    print("-" * 62)

    for table in TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}` WHERE nd_active_flag = 'Y'")
            active = cur.fetchone()[0]
            print(f"{table:<35} {total:>12,} {active:>12,}")
            rows.append({"table": table, "schema_label": label, "schema": schema,
                         "total": total, "active_y": active, "error": ""})
        except Exception as e:
            print(f"{table:<35} {'ERROR':>12}   {e}")
            rows.append({"table": table, "schema_label": label, "schema": schema,
                         "total": "", "active_y": "", "error": str(e)})

conn.close()

# ── Upload to GCS ─────────────────────────────────────────────────────────────
print(f"\nUploading to gs://{GCS_BUCKET}/{GCS_PATH} …")
buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=["table", "schema_label", "schema", "total", "active_y", "error"])
writer.writeheader()
writer.writerows(rows)

client = storage.Client()
client.bucket(GCS_BUCKET).blob(GCS_PATH).upload_from_string(
    buf.getvalue(), content_type="text/csv"
)
print("Done.")
