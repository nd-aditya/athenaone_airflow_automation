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
pip install sqlalchemy pandas snowflake-sqlalchemy PyMySQL psycopg2-binary mysql-connector-python pycryptodome
# Airflow 3 async metadata DB driver (required for PostgreSQL / default config)
pip install asyncpg
pip install "Django>=4.2,<5"
pip install jupyter_server

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

# ---- Create PostgreSQL metadata database if it doesn't exist ----
# Reads db name from sql_alchemy_conn in airflow.cfg (e.g. airflow_2)
if [[ -f "$CFG" ]] && grep -q "sql_alchemy_conn = postgresql" "$CFG"; then
  AIRFLOW_DB_NAME=$(grep '^sql_alchemy_conn = postgresql' "$CFG" | sed 's/.*\///; s/[?&#].*//' | tr -d ' \r' | head -1)
  if [[ -n "$AIRFLOW_DB_NAME" ]]; then
    echo "Ensuring PostgreSQL database '$AIRFLOW_DB_NAME' exists..."
    python - "$AIRFLOW_DB_NAME" << 'PYEOF'
import os
import sys
try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print("Warning: psycopg2 not found; skipping DB create. Install psycopg2-binary or create DB manually.")
    sys.exit(0)
dbname = sys.argv[1]
host = os.environ.get("AIRFLOW_DB_HOST", "localhost")
port = int(os.environ.get("AIRFLOW_DB_PORT", "5432"))
user = os.environ.get("AIRFLOW_DB_USER", "postgres")
password = os.environ.get("PGPASSWORD", "")
try:
    conn = psycopg2.connect(host=host, port=port, user=user, password=password or None, dbname="postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    if cur.fetchone() is None:
        if not all(c.isalnum() or c == '_' for c in dbname):
            raise ValueError("Invalid database name (use only letters, digits, underscore)")
        cur.execute("CREATE DATABASE " + dbname)
        print("Created database:", dbname)
    else:
        print("Database already exists:", dbname)
    conn.close()
except Exception as e:
    print("Warning: could not create DB:", e, "- create it manually or set AIRFLOW_DB_* / PGPASSWORD")
PYEOF
  fi
fi

echo "Initializing Airflow DB..."
airflow db migrate

echo "--------------------------------------"
echo "Setup complete. To start Airflow, run: ./run_airflow.sh"
echo "--------------------------------------"
