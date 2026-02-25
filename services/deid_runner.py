"""
Run the deidentification pipeline from Airflow: write override file, nd_auto_increment_id,
register_dump. Mapping/master is in mapping_master_service (separate DAG task). Deid task
creation and wait are separate so validation can be added in between.
Assumes spine_worker is running elsewhere. Requires Django/Deid_service on path and configured.
"""
import json
import os
import sys
import time
from datetime import datetime


# Path to Deid_service Django project (manage.py lives in deIdentification/)
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


def _override_file_path():
    from django.conf import settings
    return os.path.join(settings.BASE_DIR, "config", "airflow_deid_override.json")


def _write_override_file(diff_schema: str):
    """Write config/airflow_deid_override.json so SchedulerConfig uses diff_<date> and diff_<date>_deid."""
    config_dir = os.path.dirname(_override_file_path())
    os.makedirs(config_dir, exist_ok=True)
    path = _override_file_path()
    data = {
        "current_schema": diff_schema,
        "deid_schema": f"{diff_schema}_deid",
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _remove_override_file():
    """Remove override file so next UI/manual run uses SchedulerConfig from DB."""
    try:
        path = _override_file_path()
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


def run_deid_pipeline_for_airflow(diff_schema: str) -> dict:
    """
    Write override file, run nd_auto_increment_id, register_dump. Does not run mapping/master
    or create deid tasks; those are separate DAG tasks (mapping_master_service, then create_deid_tasks_for_queue).
    Returns dict with queue_id, diff_schema, tables_registered for XCom.
    """
    _setup_django()
    _write_override_file(diff_schema)

    from nd_api_v2.models.scheduler_config import SchedulerConfig
    from nd_api_v2.services.register_dump import register_dump_in_queue

    # 1. Update nd_auto_increment_id for diff schema
    from nd_api_v2.services.athenaone.update_nd_auto_inc_id import main as update_nd_auto_inc_main
    update_nd_auto_inc_main()

    # 2. Register dump (creates queue and tables with ranges)
    scheduler_config = SchedulerConfig.objects.last()
    if scheduler_config is None:
        raise RuntimeError("SchedulerConfig not found. Configure incremental pipeline in UI or DB.")
    connection_string = scheduler_config.get_source_connection_str()
    date_str = diff_schema.replace("diff_", "")
    try:
        dump_date = datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        dump_date = datetime.now().date()
    dump_date_str = dump_date.strftime("%Y-%m-%d")
    results, incremental_queue = register_dump_in_queue(connection_string, dump_date_str)
    queue_id = incremental_queue.id

    return {
        "queue_id": queue_id,
        "diff_schema": diff_schema,
        "tables_registered": len(results),
    }


def create_deid_tasks_for_queue(queue_id: int) -> dict:
    """
    Create deid tasks for all tables in the given queue. Call after mapping/master (and any validation).
    Returns dict with queue_id and count for XCom. Workers (e.g. spine_worker) will process the tasks.
    """
    _setup_django()
    from nd_api_v2.views.operations.deid import create_deid_tasks_for_tables
    from nd_api_v2.models import Table

    table_ids = list(Table.objects.filter(incremental_queue_id=queue_id).values_list("id", flat=True))
    create_deid_tasks_for_tables(queue_id, table_ids)
    return {"queue_id": queue_id, "deid_tasks_created_for_tables": len(table_ids)}


def wait_for_deid_completion(queue_id: int, poll_interval_seconds: int = 30, timeout_seconds: int = 86400) -> dict:
    """
    Poll until all worker tasks for this queue's deid chains are in a terminal state.
    Assumes spine_worker (or workers) are running elsewhere.
    Returns summary dict. Raises if timeout.
    """
    _setup_django()
    from worker.models import Task, Chain
    from worker.models.helper import ComputationStatus

    terminal_statuses = (ComputationStatus.COMPLETED, ComputationStatus.FAILURE, ComputationStatus.INTERRUPTED)
    start = time.time()

    while True:
        chains = Chain.objects.filter(reference_uuid__startswith=f"db_{queue_id}_")
        if not chains.exists():
            return {"queue_id": queue_id, "status": "no_chains", "message": "No chains found for queue (deid tasks may not be created yet)."}

        chain_ids = list(chains.values_list("id", flat=True))
        tasks = Task.objects.filter(chain_id__in=chain_ids)
        total = tasks.count()
        if total == 0:
            return {"queue_id": queue_id, "status": "no_tasks", "message": "No tasks found for queue chains."}

        pending = tasks.exclude(status__in=terminal_statuses).count()
        if pending == 0:
            completed = tasks.filter(status=ComputationStatus.COMPLETED).count()
            failed = tasks.filter(status=ComputationStatus.FAILURE).count()
            _remove_override_file()
            return {
                "queue_id": queue_id,
                "status": "done",
                "total_tasks": total,
                "completed": completed,
                "failed": failed,
                "elapsed_seconds": round(time.time() - start, 1),
            }

        if time.time() - start > timeout_seconds:
            raise TimeoutError(
                f"Deid wait timed out after {timeout_seconds}s. Queue {queue_id}: {pending}/{total} tasks still pending."
            )
        time.sleep(poll_interval_seconds)
