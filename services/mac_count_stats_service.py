"""
Maintains a merge_stats table in deidentified_merged on MAC.
After each DAG2/DAG4 run, inserts per-table counts and uploads the full table to GCS.

Table: deidentified_merged.merge_stats
GCS:   gs://<GCP_BUCKET>/merge_stats/<MMDDYYYY>/merge_stats.csv

On GCP side: restore CSV into merge_stats table, add gcp_total/gcp_active_y columns,
compute discrepancy.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime

from sqlalchemy import create_engine, text

from services.config import (
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_HOST,
    DEIDENTIFIED_SCHEMA,
    GCP_BUCKET,
    MERGE_STATS_GCS_FOLDER,
)

PRIORITY_TABLES = [
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

_CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{DEIDENTIFIED_SCHEMA}`.`merge_stats` (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    snapshot_date DATE         NOT NULL,
    dag_id        VARCHAR(100) NOT NULL,
    table_name    VARCHAR(200) NOT NULL,
    mac_total     BIGINT       NOT NULL DEFAULT 0,
    mac_active_y  BIGINT       NOT NULL DEFAULT 0,
    gcp_total     BIGINT       NULL,
    gcp_active_y  BIGINT       NULL,
    discrepancy   TINYINT      NULL COMMENT '1 = counts differ, 0 = match, NULL = not yet compared',
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_snapshot_dag_table (snapshot_date, dag_id, table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def upload_merge_stats_to_gcs(dag_id: str, run_date: datetime | None = None) -> dict:
    """
    1. Create merge_stats table if it doesn't exist.
    2. INSERT counts for each priority table (INSERT ... ON DUPLICATE KEY UPDATE
       so re-running the same DAG on the same date overwrites instead of duplicating).
    3. Upload the full merge_stats table as CSV to GCS.

    run_date: pass DAG logical_date so the folder is pinned to the scheduled run date,
              not the wall-clock time of the upload task.
    Returns summary dict for Airflow XCom.
    """
    effective_date = run_date or datetime.now()
    snapshot_date  = effective_date.strftime("%Y-%m-%d")
    date_folder    = effective_date.strftime("%m%d%Y")
    gcs_path       = f"{MERGE_STATS_GCS_FOLDER}/{date_folder}/merge_stats.csv"

    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/",
        pool_pre_ping=True,
    )
    errors = []

    with engine.connect() as conn:
        # 1. Ensure table exists
        conn.execute(text(_CREATE_TABLE_SQL))
        conn.commit()

        # 2. Delete all previous rows — keep only the latest run regardless of dag_id
        conn.execute(text(f"DELETE FROM `{DEIDENTIFIED_SCHEMA}`.`merge_stats`"))
        conn.commit()

        # 3. Insert counts for each priority table
        for table in PRIORITY_TABLES:
            try:
                total = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{DEIDENTIFIED_SCHEMA}`.`{table}`")
                ).scalar() or 0
                active = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{DEIDENTIFIED_SCHEMA}`.`{table}` WHERE nd_active_flag = 'Y'")
                ).scalar() or 0
                conn.execute(text(f"""
                    INSERT INTO `{DEIDENTIFIED_SCHEMA}`.`merge_stats`
                        (snapshot_date, dag_id, table_name, mac_total, mac_active_y)
                    VALUES
                        (:sd, :dag, :tbl, :total, :active)
                """), {"sd": snapshot_date, "dag": dag_id, "tbl": table.upper(),
                       "total": int(total), "active": int(active)})
            except Exception as e:
                errors.append({"table": table, "error": str(e)})

        conn.commit()

        # 4. Read current snapshot rows to upload as CSV
        result = conn.execute(text(f"""
            SELECT snapshot_date, dag_id, table_name,
                   mac_total, mac_active_y,
                   gcp_total, gcp_active_y, discrepancy, created_at
            FROM `{DEIDENTIFIED_SCHEMA}`.`merge_stats`
            ORDER BY table_name
        """))
        all_rows = result.fetchall()
        columns  = list(result.keys())

    engine.dispose()

    # Upload CSV to GCS via gsutil (same credentials as dump service)
    import os
    import subprocess
    import tempfile

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in all_rows:
        writer.writerow(row)

    gcs_uri = f"gs://{GCP_BUCKET}/{gcs_path}"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
        tmp.write(buf.getvalue())
        tmp_path = tmp.name
    try:
        subprocess.run(
            ["gsutil", "cp", tmp_path, gcs_uri],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        os.unlink(tmp_path)
    print(f"[merge_stats] {len(PRIORITY_TABLES) - len(errors)} tables inserted → {DEIDENTIFIED_SCHEMA}.merge_stats")
    print(f"[merge_stats] Full table uploaded → {gcs_uri}")
    if errors:
        print(f"[merge_stats] Errors: {errors}")

    return {
        "gcs_uri":        gcs_uri,
        "date_folder":    date_folder,
        "tables_counted": len(PRIORITY_TABLES) - len(errors),
        "tables_errored": errors,
    }
