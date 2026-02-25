#!/bin/bash
#
# Start deidentification workers (Mac: new Terminal windows). Prefer the repo-root
# script for background workers: from athenaone_airflow_automation run
#   ./start_worker.sh [N]
# That uses dynamic paths and ensures workers see config/airflow_deid_override.json.
#
# This script uses the directory containing this file to find deIdentification/manage.py.
# Override with CONDA_ENV=name or VENV_PATH=/path if needed.

n=${1:-1}

# Script is in Deid_service/deidentification/; manage.py is in deIdentification/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/deIdentification"
VENV_PATH="${VENV_PATH:-py39}"

if [[ ! -f "$PROJECT_DIR/manage.py" ]]; then
  echo "Error: manage.py not found at $PROJECT_DIR"
  exit 1
fi

for ((i = 1; i <= n; i++)); do
  if [[ -n "$CONDA_ENV" ]]; then
    cmd="conda activate '$CONDA_ENV' && cd '$PROJECT_DIR' && python manage.py start_worker"
  else
    cmd="conda activate '$VENV_PATH' && cd '$PROJECT_DIR' && python manage.py start_worker"
  fi
  # Escape for AppleScript: backslash and double-quote
  cmd_escaped="${cmd//\\/\\\\}"
  cmd_escaped="${cmd_escaped//\"/\\\"}"
  osascript -e "tell application \"Terminal\" to do script \"$cmd_escaped\""
  sleep 1
done

echo "$n workers started in separate Terminal windows."
