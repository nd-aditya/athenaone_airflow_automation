"""
Adhoc deidentification DAG: register dump from ADHOC_SCHEMA, deid only ADHOC_TABLES,
output to DEIDENTIFIED_ADHOC_SCHEMA. No mapping/master, no merge. Config in services/config.py.
"""
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

from services.config import (
    ADHOC_SCHEMA,
    DEIDENTIFIED_ADHOC_SCHEMA,
    ADHOC_TABLES,
    DEID_WORKERS,
    DEID_CONDA_ENV,
)
from services.deid_runner import (
    run_adhoc_deid_pipeline_for_airflow,
    create_deid_tasks_for_queue,
    wait_for_deid_completion,
    get_table_ids_for_queue_by_names,
)
from services.worker_lifecycle import clear_worker_logs, start_workers, stop_workers


with DAG(
    dag_id="Athenaone_Adhoc_Deid",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["athenaone", "adhoc", "deid"],
) as dag_adhoc_deid:

    @task
    def run_adhoc_deid_pipeline() -> dict:
        return run_adhoc_deid_pipeline_for_airflow(ADHOC_SCHEMA, DEIDENTIFIED_ADHOC_SCHEMA)

    @task
    def start_deid_workers(pipe_result: dict) -> dict:
        if not pipe_result or "queue_id" not in pipe_result:
            raise ValueError("run_adhoc_deid_pipeline did not return queue_id")
        out = clear_worker_logs()
        out_start = start_workers(n=DEID_WORKERS, conda_env=DEID_CONDA_ENV)
        return {**(pipe_result or {}), "cleared_logs": out.get("cleared", []), **out_start}

    @task
    def get_adhoc_table_ids_and_create_tasks(pipe_result: dict) -> dict:
        if not pipe_result or "queue_id" not in pipe_result:
            raise ValueError("pipe_result missing queue_id")
        if not ADHOC_TABLES:
            return {**(pipe_result or {}), "table_ids": [], "deid_tasks_created_for_tables": 0}
        table_ids = get_table_ids_for_queue_by_names(pipe_result["queue_id"], ADHOC_TABLES)
        out = create_deid_tasks_for_queue(pipe_result["queue_id"], table_ids=table_ids)
        return {**out, "table_ids": table_ids}

    @task
    def wait_for_deid(deid_result: dict) -> dict:
        if not deid_result or "queue_id" not in deid_result or "table_ids" not in deid_result:
            raise ValueError("deid_result missing queue_id or table_ids")
        if not deid_result["table_ids"]:
            return deid_result
        return wait_for_deid_completion(deid_result["queue_id"], table_ids=deid_result["table_ids"])

    @task
    def stop_deid_workers(_: dict) -> dict:
        return stop_workers()

    run_task = run_adhoc_deid_pipeline()
    start_workers_task = start_deid_workers(run_task)
    create_tasks_task = get_adhoc_table_ids_and_create_tasks(start_workers_task)
    wait_task = wait_for_deid(create_tasks_task)
    stop_task = stop_deid_workers(wait_task)
    run_task >> start_workers_task >> create_tasks_task >> wait_task >> stop_task
