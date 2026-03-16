from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Path to your project (adjust if different)
PROJECT_DIR = "/Users/adityaneuroAI/dashboard_automation"

with DAG(
    dag_id="dashboard_table_mysql",
    start_date=datetime(2026, 1, 1),
    schedule= None,  # or "@hourly", "0 2 * * *", etc.
    catchup=False,
    tags=["dashboard", "mysql", "reporting"],
) as dag:
    run_dashboard_sql = BashOperator(
        task_id="run_dashboard_table_mysql",
        bash_command="python dashboard_table_mysql.py",
        cwd=PROJECT_DIR,
    )