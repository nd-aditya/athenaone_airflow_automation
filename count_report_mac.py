"""Run on MAC. Prints COUNT(*) and COUNT(*) WHERE nd_active_flag='Y' for each priority table."""
import pymysql

HOST     = "localhost"
USER     = "ndadmin"
PASSWORD = "ndADMIN%402025"
SCHEMA   = "deidentified_merged"

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

conn = pymysql.connect(host=HOST, user=USER, password=PASSWORD, database=SCHEMA)
cur  = conn.cursor()

print(f"\n{'Table':<35} {'Total':>12} {'Active (Y)':>12}")
print("-" * 62)

for table in TABLES:
    try:
        cur.execute(f"SELECT COUNT(*) FROM `{table}`")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE nd_active_flag = 'Y'")
        active = cur.fetchone()[0]
        print(f"{table:<35} {total:>12,} {active:>12,}")
    except Exception as e:
        print(f"{table:<35} {'ERROR':>12} {str(e)}")

conn.close()
