#!/bin/bash
#
# Start deidentification workers from repo root. Workers run from the directory
# that contains manage.py so they read config/airflow_deid_override.json and
# use the correct deid schema (e.g. diff_20260225_deid) when Airflow has set it.
#
# Usage:
#   ./start_worker.sh [N]              Start N workers in background (default 1)
#   CONDA_ENV=airflow_inc ./start_worker.sh 2   Use conda env, start 2 workers
#   VENV_PATH=/path/to/venv ./start_worker.sh   Use virtualenv instead of conda
#
# Workers are started in the background. Logs go to ./worker_<i>.log in repo root.
# To stop: pkill -f "manage.py start_worker" or kill the PIDs printed at start.

set -e

# Repo root = directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
# Directory that contains manage.py (Django project root); worker must run from here
# so config/airflow_deid_override.json is found and correct deid schema is used
DEID_MANAGE_DIR="$PROJECT_ROOT/Deid_service/deidentification/deIdentification"

if [[ ! -f "$DEID_MANAGE_DIR/manage.py" ]]; then
  echo "Error: manage.py not found at $DEID_MANAGE_DIR. Check repo layout."
  exit 1
fi

# Number of workers (default 1)
n=${1:-1}
if ! [[ "$n" =~ ^[0-9]+$ ]] || [[ "$n" -lt 1 ]]; then
  echo "Usage: $0 [N]   (N = number of workers, default 1)"
  exit 1
fi

# Build run command: optional conda or venv, then cd and start_worker
# When using conda, source conda.sh first so 'conda activate' works in non-interactive subshells (e.g. when run by Airflow).
if [[ -n "$CONDA_ENV" ]]; then
  CONDA_BASE="${CONDA_BASE:-$(conda info --base 2>/dev/null)}"
  if [[ -z "$CONDA_BASE" || ! -f "$CONDA_BASE/etc/profile.d/conda.sh" ]]; then
    echo "Error: conda not available or conda.sh not found. Set CONDA_BASE or run from a shell with conda on PATH."
    exit 1
  fi
  RUN_CMD="source '$CONDA_BASE/etc/profile.d/conda.sh' && conda activate '$CONDA_ENV' && cd '$DEID_MANAGE_DIR' && python manage.py start_worker"
elif [[ -n "$VENV_PATH" ]]; then
  if [[ -d "$VENV_PATH" ]]; then
    RUN_CMD="source '$VENV_PATH/bin/activate' && cd '$DEID_MANAGE_DIR' && python manage.py start_worker"
  else
    echo "Error: VENV_PATH is set but not a directory: $VENV_PATH"
    exit 1
  fi
else
  RUN_CMD="cd '$DEID_MANAGE_DIR' && python manage.py start_worker"
fi

LOG_DIR="$PROJECT_ROOT"
echo "Starting $n worker(s) from $DEID_MANAGE_DIR"
echo "Logs: $LOG_DIR/worker_1.log ... worker_$n.log"
echo ""

for ((i = 1; i <= n; i++)); do
  LOG_FILE="$LOG_DIR/worker_$i.log"
  nohup bash -c "$RUN_CMD" >> "$LOG_FILE" 2>&1 &
  PID=$!
  echo "Worker $i started (PID $PID) -> $LOG_FILE"
  sleep 1
done

echo ""
echo "Workers running. To stop: pkill -f 'manage.py start_worker'"
