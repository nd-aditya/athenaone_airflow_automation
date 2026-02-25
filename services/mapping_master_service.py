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

    return {"queue_id": queue_id, "status": "ok"}
