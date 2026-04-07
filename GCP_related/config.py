"""
GCP restore-machine config.

This file controls ONLY the restore side of the pipeline.
The dump side (GCP_FULL_REFRESH_FLAG, GCP_BUCKET, etc.) lives in services/config.py
on each source machine.

To add a new source machine:
  1. Copy one entry in MACHINE_RESTORE_CONFIGS and change name, gcs_bucket, and schema names.
  2. Make sure the dump machine has GCP_DESTINATION_PREFIX = "nd_incremental" in its services/config.py.
  The restore DAG will automatically start watching the new bucket on the next run.
"""
import os

# =============================================================================
# SOURCE MACHINE CONFIGS
# Add one entry per dump machine. The restore DAG watches every bucket listed
# here for new data (folders containing sql_dump_stats.csv) and restores each
# independently into its own staging + incremental MySQL schemas.
#
# Fields per entry:
#   name               – human label used in logs and Airflow task output
#   gcs_bucket         – GCS bucket the dump machine uploads to
#                        (set GCP_BUCKET = this value in that machine's services/config.py)
#   gcs_watch_prefix   – folder prefix inside the bucket to watch
#                        (must match GCP_DESTINATION_PREFIX on the dump machine)
#   staging_schema     – MySQL schema for the temporary restore (wiped each run)
#   incremental_schema – MySQL schema that accumulates rows across runs until merged
# =============================================================================
MACHINE_RESTORE_CONFIGS = [
    {
        "name": "machine1",
        "gcs_bucket": "nd-platform-machine1",
        "gcs_watch_prefix": "nd_incremental",
        "staging_schema": "nd_staging_machine1",
        "incremental_schema": "nd_incremental_machine1",
        "full_deidentified_schema": "deidentified_merged_machine1",  # target for incremental merge
    },
    {
        "name": "machine2",
        "gcs_bucket": "nd-platform-machine2",
        "gcs_watch_prefix": "nd_incremental",
        "staging_schema": "nd_staging_machine2",
        "incremental_schema": "nd_incremental_machine2",
        "full_deidentified_schema": "deidentified_merged_machine2",
    },
    {
        "name": "machine3",
        "gcs_bucket": "nd-platform-machine3",
        "gcs_watch_prefix": "nd_incremental",
        "staging_schema": "nd_staging_machine3",
        "incremental_schema": "nd_incremental_machine3",
        "full_deidentified_schema": "deidentified_merged_machine3",
    },
    {
        "name": "machine4",
        "gcs_bucket": "nd-platform-machine4",
        "gcs_watch_prefix": "nd_incremental",
        "staging_schema": "nd_staging_machine4",
        "incremental_schema": "nd_incremental_machine4",
        "full_deidentified_schema": "deidentified_merged_machine4",
    },
    # ── Add more machines here ────────────────────────────────────────────────
    # {
    #     "name": "machine5",
    #     "gcs_bucket": "nd-platform-machine5",
    #     "gcs_watch_prefix": "nd_incremental",
    #     "staging_schema": "nd_staging_machine5",
    #     "incremental_schema": "nd_incremental_machine5",
    #     "full_deidentified_schema": "deidentified_merged_machine5",
    # },
]

# =============================================================================
# GCS MARKER FILES  (do not change — must match what gcp_dump_service.py writes)
# =============================================================================
GCS_DUMP_STATS_FILE  = "sql_dump_stats.csv"  # presence signals upload is complete
GCS_PROCESSED_MARKER = "_processed"          # written after restore+append → skips on next scan

# How often (minutes) the restore DAG polls every bucket for new dumps
GCS_POLL_INTERVAL_MINUTES = 30

# =============================================================================
# DB CREDENTIALS  (set as environment variables on the restore machine)
# =============================================================================
MYSQL_USER     = os.getenv("MYSQL_USER", "")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_HOST     = os.getenv("MYSQL_HOST", "")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))

# =============================================================================
# WORKER COUNTS
# =============================================================================
RESTORE_MAX_WORKERS  = 10
APPEND_MAX_WORKERS   = 10
MERGE_MAX_WORKERS    = 10
TRUNCATE_MAX_WORKERS = 10
