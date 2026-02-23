#!/usr/bin/env bash

set -e  # Exit on any error

echo "🚀 Starting Airflow 3.1.7 setup with Python 3.13..."

# ----------------------------------------
# CONFIGURABLE VARIABLES
# ----------------------------------------

PYTHON_BIN=python3.13
VENV_NAME=airflow_inc
AIRFLOW_VERSION=3.1.7
PROJECT_ROOT=$(pwd)
AIRFLOW_HOME_DIR="$PROJECT_ROOT/airflow_home"

# ----------------------------------------
# 1️⃣ Create Virtual Environment
# ----------------------------------------

echo "📦 Creating virtual environment..."
$PYTHON_BIN -m venv $VENV_NAME

echo "🔁 Activating virtual environment..."
source $VENV_NAME/bin/activate

echo "⬆️ Upgrading pip..."
pip install --upgrade pip setuptools wheel

# ----------------------------------------
# 2️⃣ Install Airflow Core
# ----------------------------------------

echo "📦 Installing Apache Airflow ${AIRFLOW_VERSION}..."
pip install "apache-airflow==${AIRFLOW_VERSION}"

# ----------------------------------------
# 3️⃣ Install Standard Extras (UI + API + auth deps)
# ----------------------------------------

echo "📦 Installing Airflow standard extras..."
pip install "apache-airflow[standard]"

# ----------------------------------------
# 4️⃣ Install Database Drivers
# ----------------------------------------

echo "📦 Installing PostgreSQL drivers..."
pip install psycopg2-binary asyncpg

# ----------------------------------------
# 5️⃣ Install FAB Provider (Auth Manager)
# ----------------------------------------

echo "📦 Installing FAB provider..."
pip install apache-airflow-providers-fab

# ----------------------------------------
# 6️⃣ Install Common Providers (Optional but Recommended)
# ----------------------------------------

echo "📦 Installing common providers..."
pip install apache-airflow-providers-snowflake
pip install apache-airflow-providers-mysql

# ----------------------------------------
# 7️⃣ Create Project Structure
# ----------------------------------------

echo "📂 Creating project structure..."
mkdir -p dags services config

# ----------------------------------------
# 8️⃣ Configure AIRFLOW_HOME
# ----------------------------------------

echo "⚙️ Setting AIRFLOW_HOME..."
export AIRFLOW_HOME=$AIRFLOW_HOME_DIR
mkdir -p $AIRFLOW_HOME

echo "AIRFLOW_HOME set to: $AIRFLOW_HOME"

# ----------------------------------------
# 9️⃣ Initialize Airflow Database
# ----------------------------------------

echo "🗄 Initializing Airflow database..."
airflow db init

echo "✅ Setup Complete!"
echo ""
echo "To activate environment later:"
echo "source $VENV_NAME/bin/activate"
echo ""
echo "To start Airflow:"
echo "export AIRFLOW_HOME=$AIRFLOW_HOME_DIR"
echo "airflow standalone"
