"""
Run on MAC.
Fetches COUNT(*) and COUNT(*) WHERE nd_active_flag='Y' for each priority table
from deidentified_merged, then uploads results to GCS.

Usage:
    python count_report_mac.py
"""
import csv
import io
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import pymysql
from google.cloud import storage

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    DEIDENTIFIED_SCHEMA,
    COUNT_REPORT_GCS_BUCKET,
    COUNT_REPORT_MAC_GCS_PATH,
)

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
print(f"Connecting to MAC MySQL ({MYSQL_HOST}/{DEIDENTIFIED_SCHEMA})…")
conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=DEIDENTIFIED_SCHEMA)
cur  = conn.cursor()

rows = []
print(f"\n{'Table':<35} {'Total':>12} {'Active (Y)':>12}")
print("-" * 62)
for table in TABLES:
    try:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE nd_active_flag = 'Y'")
        active = cur.fetchone()[0]
        print(f"{table:<35} {total:>12,} {active:>12,}")
        rows.append({"table": table, "total": total, "active_y": active, "error": ""})
    except Exception as e:
        print(f"{table:<35} {'ERROR':>12}   {e}")
        rows.append({"table": table, "total": "", "active_y": "", "error": str(e)})

conn.close()

# ── Upload to GCS ─────────────────────────────────────────────────────────────
print(f"\nUploading to gs://{COUNT_REPORT_GCS_BUCKET}/{COUNT_REPORT_MAC_GCS_PATH} …")
buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=["table", "total", "active_y", "error"])
writer.writeheader()
writer.writerows(rows)

client = storage.Client()
client.bucket(COUNT_REPORT_GCS_BUCKET).blob(COUNT_REPORT_MAC_GCS_PATH).upload_from_string(
    buf.getvalue(), content_type="text/csv"
)
print("Done. Run count_report_compare.py on MAC to generate the comparison Excel.")
