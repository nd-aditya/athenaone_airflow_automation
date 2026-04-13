"""Run on GCP machine. Prints COUNT(*) and COUNT(*) WHERE nd_active_flag='Y' for each priority table across all provider schemas."""
import pymysql

HOST     = "localhost"
USER     = "nd-root-mysql"
PASSWORD = "kmsamd89undsd4"

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

conn = pymysql.connect(host=HOST, user=USER, password=PASSWORD)
cur  = conn.cursor()

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
        except Exception as e:
            print(f"{table:<35} {'ERROR':>12} {str(e)}")

conn.close()
