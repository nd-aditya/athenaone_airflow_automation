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


def _ensure_deid_database_exists(deid_schema: str):
    """
    Create the deidentified MySQL database if it does not exist.
    Uses SchedulerConfig.get_deid_connection_str() (with override already applied) to get
    server details, then connects without a database and runs CREATE DATABASE IF NOT EXISTS.
    """
    from nd_api_v2.models.scheduler_config import SchedulerConfig
    from sqlalchemy.engine.url import make_url
    from sqlalchemy import create_engine, text

    try:
        from deIdentification.nd_logger import nd_logger
    except Exception:
        nd_logger = None

    scheduler_config = SchedulerConfig.objects.last()
    if scheduler_config is None:
        return
    conn_str = scheduler_config.get_deid_connection_str()
    try:
        url = make_url(conn_str)
        if url.drivername and "mysql" not in url.drivername:
            return
        url_no_db = url.set(database=None)
        engine = create_engine(url_no_db, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{deid_schema}` DEFAULT CHARACTER SET utf8mb4"))
            conn.commit()
        if nd_logger:
            nd_logger.info(f"Deid database '{deid_schema}' ensured (created if not existed).")
    except Exception as e:
        if nd_logger:
            nd_logger.exception(f"Failed to create deid database '{deid_schema}': {e}")
        # Do not re-raise so pipeline can continue; fix DB permissions if needed


def _update_table_nd_ranges_from_diff_schema(diff_schema: str, queue_id: int):
    """
    For each Table in the given queue, set nd_auto_increment_start_value and
    nd_auto_increment_end_value from min/max(nd_auto_increment_id) in the diff schema.
    Connection to diff schema must already be configured (override file written).
    """
    from nd_api_v2.models.scheduler_config import SchedulerConfig
    from nd_api_v2.models import Table
    from sqlalchemy import create_engine, text

    scheduler_config = SchedulerConfig.objects.last()
    if scheduler_config is None:
        return
    connection_string = scheduler_config.get_source_connection_str()
    engine = create_engine(connection_string, pool_pre_ping=True)
    tables = Table.objects.filter(incremental_queue_id=queue_id).select_related("metadata")
    for table_obj in tables:
        table_name = table_obj.metadata.table_name if table_obj.metadata else getattr(table_obj, "metadata_table_name", None)
        if not table_name:
            continue
        try:
            with engine.connect() as conn:
                row = conn.execute(
                    text(f"SELECT MIN(`nd_auto_increment_id`), MAX(`nd_auto_increment_id`) FROM `{table_name}`")
                ).fetchone()
            if row and row[0] is not None and row[1] is not None:
                table_obj.nd_auto_increment_start_value = int(row[0])
                table_obj.nd_auto_increment_end_value = int(row[1])
                table_obj.save()
        except Exception:
            pass
    engine.dispose()


def run_deid_pipeline_for_airflow(diff_schema: str) -> dict:
    """
    Write override file, ensure deid DB, delete old queues, register_dump, set nd ranges from diff.
    Does not run mapping/master or create deid tasks; those are separate DAG tasks.
    Returns dict with queue_id, diff_schema, tables_registered for XCom.
    """
    _setup_django()
    _write_override_file(diff_schema)

    # Create deidentified schema (e.g. diff_20260225_deid) if it does not exist
    deid_schema = f"{diff_schema}_deid"
    _ensure_deid_database_exists(deid_schema)

    from nd_api_v2.models.scheduler_config import SchedulerConfig
    from nd_api_v2.models.incremental_queue import IncrementalQueue
    from nd_api_v2.services.register_dump import register_dump_in_queue

    # Delete old queues so this run has a clean set (same as manual register_dump.py)
    IncrementalQueue.objects.all().delete()

    # Register dump (creates queue and Table records from diff schema)
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

    # Set nd_auto_increment_start_value / end_value from min/max(nd_auto_increment_id) in diff schema
    _update_table_nd_ranges_from_diff_schema(diff_schema, queue_id)

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
