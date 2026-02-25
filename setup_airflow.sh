#!/bin/bash
# Setup Airflow with conda environment airflow_inc.
# Run from repo root: ./setup_airflow.sh

set -e

CONDA_ENV="airflow_inc"
# Project root = directory where this script lives
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_HOME_DIR="$PROJECT_ROOT/airflow_home"

echo "--------------------------------------"
echo "Project root: $PROJECT_ROOT"
echo "AIRFLOW_HOME: $AIRFLOW_HOME_DIR"
echo "Conda env: $CONDA_ENV"
echo "--------------------------------------"

# ---- Load conda ----
if ! command -v conda &>/dev/null; then
  echo "Error: conda not found. Install Miniconda/Anaconda first."
  exit 1
fi
source "$(conda info --base)/etc/profile.d/conda.sh"

# ---- Create conda env if missing ----
if conda env list | grep -q "^${CONDA_ENV} "; then
  echo "Conda env '$CONDA_ENV' already exists. Activating."
else
  echo "Creating conda env '$CONDA_ENV' with Python 3.13..."
  conda create -n "$CONDA_ENV" python=3.13 -y
fi
conda activate "$CONDA_ENV"

# ---- Install Airflow and dependencies ----
echo "Installing Apache Airflow and project dependencies..."
pip install --upgrade pip
pip install "apache-airflow==3.1.7"
pip install sqlalchemy pandas snowflake-sqlalchemy PyMySQL
# Airflow 3 async metadata DB driver (required for PostgreSQL / default config)
pip install asyncpg

# Optional: install Deid_service deps if you run deid from this env
DEID_REQ="$PROJECT_ROOT/Deid_service/deidentification/requirements.txt"
if [[ -f "$DEID_REQ" ]]; then
  echo "Installing Deid_service requirements..."
  pip install -r "$DEID_REQ" || true
fi

# ---- Set AIRFLOW_HOME and init DB ----
export AIRFLOW_HOME="$AIRFLOW_HOME_DIR"
mkdir -p "$AIRFLOW_HOME"

# Use portable dags_folder in airflow.cfg (relative to AIRFLOW_HOME)
CFG="$AIRFLOW_HOME/airflow.cfg"
if [[ -f "$CFG" ]]; then
  # Replace absolute dags_folder with relative so it works on any machine
  if grep -q "dags_folder = " "$CFG"; then
    sed -i.bak 's|^dags_folder = .*|dags_folder = dags|' "$CFG" 2>/dev/null || \
    sed -i '' 's|^dags_folder = .*|dags_folder = dags|' "$CFG"
  fi
fi

echo "Initializing Airflow DB..."
airflow db init

echo "--------------------------------------"
echo "Setup complete. To start Airflow, run: ./run_airflow.sh"
echo "--------------------------------------"
