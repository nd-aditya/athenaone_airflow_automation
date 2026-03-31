"""
Update mapping and master tables for a given queue_id.
Separate service so a validation step can be added between this and deidentification in the DAG.
Expects the Airflow override file to already be written by run_deid_pipeline (same run).
"""
import os
import sys

# Path to Deid_service Django project
_DEID_PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "Deid_service", "deidentification", "deIdentification")
)


def _setup_django():
    """Add Deid project to path and run django.setup()."""
    if _DEID_PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _DEID_PROJECT_ROOT)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
    os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
    import django
    django.setup()


def update_mapping_and_master_tables(queue_id: int) -> dict:
    """
    Run patient mapping, encounter mapping, and master table generation for the given queue.
    Uses SchedulerConfig (and Airflow override file if present from previous task).
    Returns dict with queue_id and status for XCom.
    """
    _setup_django()
    from nd_api_v2.services.mapping_master import (
        run_patient_mapping_generation_task,
        run_encounter_mapping_generation_task,
        run_master_table_generation_task,
    )

    run_patient_mapping_generation_task(queue_id)
    run_encounter_mapping_generation_task(queue_id)
    run_master_table_generation_task(queue_id)

    import importlib, services.config as _cfg
    importlib.reload(_cfg)
    from services.config import ENABLE_MASTER_INSURANCE_DEDUP, MASTER_INSURANCE_DEDUP_PATIENT_IDS
    print(f"[dedup] ENABLE_MASTER_INSURANCE_DEDUP={ENABLE_MASTER_INSURANCE_DEDUP}")
    if ENABLE_MASTER_INSURANCE_DEDUP and MASTER_INSURANCE_DEDUP_PATIENT_IDS:
        _dedup_master_insurance_table(MASTER_INSURANCE_DEDUP_PATIENT_IDS)

    return {"queue_id": queue_id, "status": "ok"}


def _dedup_master_insurance_table(patient_ids: list):
    """
    For each nd_patient_id in patient_ids, keep one row at random in
    master_prod.master_insurance_table and delete the rest.
    Only runs when ENABLE_MASTER_INSURANCE_DEDUP = True.
    """
    from sqlalchemy import create_engine, text
    from services.config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST

    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/master_prod",
        pool_pre_ping=True,
    )
    with engine.connect() as conn:
        for pid in patient_ids:
            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS _keep_insurance"))
            conn.execute(text("""
                CREATE TEMPORARY TABLE _keep_insurance AS
                SELECT * FROM master_prod.master_insurance_table
                WHERE nd_patient_id = :pid
                LIMIT 1
            """), {"pid": pid})

            conn.execute(text("""
                DELETE FROM master_prod.master_insurance_table
                WHERE nd_patient_id = :pid
            """), {"pid": pid})

            conn.execute(text("""
                INSERT INTO master_prod.master_insurance_table
                SELECT * FROM _keep_insurance
            """))

            conn.execute(text("DROP TEMPORARY TABLE IF EXISTS _keep_insurance"))
            conn.commit()

    engine.dispose()
