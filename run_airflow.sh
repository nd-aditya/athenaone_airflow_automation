#!/bin/bash

# Stop on error
set -e

# ---- CONFIG ----
CONDA_ENV="airflow_inc"
AIRFLOW_PORT=8890
# Project root = directory where this script lives
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_HOME="$PROJECT_ROOT/airflow_home"

# ---- LOAD CONDA ----
source "$(conda info --base)/etc/profile.d/conda.sh"

# ---- ACTIVATE ENV ----
conda activate "$CONDA_ENV"

export AIRFLOW_HOME
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH:-}"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# Absolute path so scheduler/DAG processor always find your DAGs (avoids relative-path issues)
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_HOME}/dags"

echo "--------------------------------------"
echo "Using CONDA ENV: $CONDA_ENV"
echo "Using AIRFLOW_HOME: $AIRFLOW_HOME"
echo "DAGS_FOLDER: $AIRFLOW__CORE__DAGS_FOLDER"
echo "Starting Airflow on port $AIRFLOW_PORT"
echo "--------------------------------------"

# Run from project root so DAG imports (e.g. services) resolve correctly
cd "$PROJECT_ROOT"

# ---- START SERVICES ----
airflow scheduler &
SCHEDULER_PID=$!

sleep 3

airflow dag-processor &
DAG_PROCESSOR_PID=$!

sleep 3

airflow api-server &
API_PID=$!

echo "--------------------------------------"
echo "Airflow started."
echo "Scheduler PID: $SCHEDULER_PID"
echo "DAG Processor PID: $DAG_PROCESSOR_PID"
echo "API Server PID: $API_PID"
echo "UI: http://localhost:$AIRFLOW_PORT"
echo "--------------------------------------"

# Keep script alive
wait
