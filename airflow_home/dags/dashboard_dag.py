"""
Dashboard reporting DAG: runs Dashboard/dashboard_table.py to execute SQL from
Dashboard/queries.sql (with config_dashboard.py for DB and schema mapping).
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Project root: airflow_home/dags/dashboard_dag.py -> go up to project root
_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
_AIRFLOW_HOME = os.path.dirname(_DAG_DIR)
PROJECT_ROOT = os.path.dirname(_AIRFLOW_HOME)
DASHBOARD_DIR = os.path.join(PROJECT_ROOT, "Dashboard")

with DAG(
    dag_id="dashboard_reporting",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dashboard", "reporting"],
) as dag_dashboard:

    run_dashboard_queries = BashOperator(
        task_id="run_dashboard_queries",
        bash_command=f"cd {DASHBOARD_DIR!r} && python dashboard_table.py",
        cwd=DASHBOARD_DIR,
    )
