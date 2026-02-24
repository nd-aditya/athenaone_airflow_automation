#!/usr/bin/env python3
"""
Manual Pipeline Runner - Complete 9-Step Data Pipeline
Executes all pipeline steps sequentially without Airflow
With Google Chat webhook notifications
Usage: python scripts/manual_pipeline_runner.py
"""

import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, text

# Add project root to path to import config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import centralized logger
from nd_api_v2.services.incrementalflow.pipeline_logger import (
    get_logger, log_step, log_success, log_error, 
    log_info, log_header, log_summary
)
from nd_api_v2.services.register_dump import register_dump_in_queue
# Import Google Chat notifier
from nd_api_v2.services.incrementalflow.google_chat_notifier import get_notifier

# Setup logging
logger = get_logger("manual_pipeline", "logs")

# Setup notifier
notifier = get_notifier()

# Import config from Django model
from config_loader import (
    INCREMENTAL_SCHEMA, MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST,
    NOTIFY_ON_STEP, NOTIFY_ON_FAILURE, NOTIFY_ON_START, NOTIFY_ON_SUCCESS
)
# Create a config-like object for backward compatibility
class Config:
    INCREMENTAL_SCHEMA = INCREMENTAL_SCHEMA
    MYSQL_USER = MYSQL_USER
    MYSQL_PASSWORD = MYSQL_PASSWORD
    MYSQL_HOST = MYSQL_HOST
    NOTIFY_ON_STEP = NOTIFY_ON_STEP
    NOTIFY_ON_FAILURE = NOTIFY_ON_FAILURE
    NOTIFY_ON_START = NOTIFY_ON_START
    NOTIFY_ON_SUCCESS = NOTIFY_ON_SUCCESS

config = Config()

def execute_step(step_num, total_steps, step_name, step_function):
    """Execute a pipeline step with error handling and notifications"""
    
    # Print for immediate dashboard output
    print(f"\n{'='*80}", flush=True)
    print(f"STEP {step_num}/{total_steps}: {step_name.upper()}", flush=True)
    print(f"{'='*80}", flush=True)
    
    # Log for file
    log_step(step_num, total_steps, step_name)
    
    # Notify step start (optional - can be noisy)
    if hasattr(config, 'NOTIFY_ON_STEP') and config.NOTIFY_ON_STEP:
        notifier.send_step_progress(step_num, total_steps, step_name, "running")
    
    try:
        step_function()
        
        # Print success for dashboard
        print(f"✅ Step {step_num} completed successfully", flush=True)
        
        # Log success
        log_success(f"Step {step_num} completed successfully")
        
        # Notify step completion
        if hasattr(config, 'NOTIFY_ON_STEP') and config.NOTIFY_ON_STEP:
            notifier.send_step_progress(step_num, total_steps, step_name, "completed")
        
        return True
    except Exception as e:
        # Print error for dashboard
        print(f"❌ Step {step_num} failed: {e}", flush=True)
        
        # Log error
        log_error(f"Step {step_num} failed: {e}")
        
        # Notify failure
        if hasattr(config, 'NOTIFY_ON_FAILURE') and config.NOTIFY_ON_FAILURE:
            notifier.send_pipeline_failure(step_num, step_name, str(e))
        
        return False


def main():
    """Execute complete pipeline manually"""
    
    start_time = datetime.now()
    
    # Print for immediate dashboard output
    print("\n" + "="*80, flush=True)
    print("🚀 STARTING MANUAL PIPELINE EXECUTION", flush=True)
    print("="*80 + "\n", flush=True)
    
    # Log for file
    log_header("🚀 STARTING MANUAL PIPELINE EXECUTION")
    
    # Send start notification
    if hasattr(config, 'NOTIFY_ON_START') and config.NOTIFY_ON_START:
        notifier.send_pipeline_start(schema=config.INCREMENTAL_SCHEMA)
    
    # Ensure database exists before importing modules (they connect at import time)
    print(f"🔍 Checking/creating database {config.INCREMENTAL_SCHEMA}...", flush=True)
    try:
        temp_engine = create_engine(f"mysql+pymysql://{config.MYSQL_USER}:{config.MYSQL_PASSWORD}@{config.MYSQL_HOST}/")
        with temp_engine.connect() as conn:
            # Create database if it doesn't exist (needed for module imports)
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{config.INCREMENTAL_SCHEMA}`"))
            print(f"✅ Database {config.INCREMENTAL_SCHEMA} is ready", flush=True)
        temp_engine.dispose()
    except Exception as e:
        print(f"❌ Failed to prepare database: {e}", flush=True)
        log_error(f"Failed to prepare database: {e}")
        notifier.send_pipeline_failure(0, "Database Preparation", str(e))
        sys.exit(1)
    
    # Import all required modules
    print("📦 Importing pipeline modules...", flush=True)
    try:
        from snowflake_to_mysql_extraction import main as extract_main
        from fixing_two_tables import run_alter_commands
        from sf_to_mysql_two_tables import main as two_tables_main
        from appointment_alter import main as appointment_alter_main
        from sf_to_mysql_scheduling_schema import main as scheduling_main
        from sf_to_mysql_financials_schema import main as financials_main
        from nd_extracted_date_column import main as add_date_main
        from update_nd_auto_inc_id import main as update_nd_id_main
        from merging import main as merge_main
        print("✅ All modules imported successfully", flush=True)
    except ImportError as e:
        print(f"❌ Failed to import modules: {e}", flush=True)
        log_error(f"Failed to import required modules: {e}")
        log_error("Make sure all script files are in the scripts/ directory")
        notifier.send_pipeline_failure(0, "Module Import", str(e))
        sys.exit(1)
    
    # Step 1: Recreate Schema
    def step1():
        temp_engine = create_engine(f"mysql+pymysql://{config.MYSQL_USER}:{config.MYSQL_PASSWORD}@{config.MYSQL_HOST}/")
        with temp_engine.connect() as conn:
            print(f"🗑️ Dropping schema: {config.INCREMENTAL_SCHEMA}", flush=True)
            log_info(f"🗑️ Dropping schema: {config.INCREMENTAL_SCHEMA}")
            conn.execute(text(f"DROP DATABASE IF EXISTS `{config.INCREMENTAL_SCHEMA}`"))
            print(f"🆕 Creating fresh schema: {config.INCREMENTAL_SCHEMA}", flush=True)
            log_info(f"🆕 Creating fresh schema: {config.INCREMENTAL_SCHEMA}")
            conn.execute(text(f"CREATE DATABASE `{config.INCREMENTAL_SCHEMA}`"))
            print(f"✅ Schema {config.INCREMENTAL_SCHEMA} recreated successfully", flush=True)
            log_success(f"Schema {config.INCREMENTAL_SCHEMA} recreated successfully")
    
    if not execute_step(1, 10, "Recreate Incremental Schema", step1):
        sys.exit(1)
    
    # Step 2: Extract from Snowflake
    if not execute_step(2, 10, "Extract from Snowflake", extract_main):
        sys.exit(1)
    
    # Step 3: Fix Two Tables
    if not execute_step(3, 10, "Fix Two Tables", run_alter_commands):
        sys.exit(1)
    
    # Step 4: Extract Two Tables
    if not execute_step(4, 10, "Extract Two Tables", two_tables_main):
        sys.exit(1)
    
    # Step 5: Appointment Table Alter
    if not execute_step(5, 10, "Appointment Table Alter", appointment_alter_main):
        sys.exit(1)
    
    # Step 6: Extract Scheduling Schema
    if not execute_step(6, 10, "Extract Scheduling Schema", scheduling_main):
        sys.exit(1)
    
    # Step 7: Extract Financials Schema
    if not execute_step(7, 10, "Extract Financials Schema", financials_main):
        sys.exit(1)
    
    # Step 8: Add Extraction Date
    if not execute_step(8, 10, "Add Extraction Date", add_date_main):
        sys.exit(1)
    
    # Step 9: Update ND Auto Increment IDs
    if not execute_step(9, 10, "Update ND Auto Increment IDs", update_nd_id_main):
        sys.exit(1)
    
    # Step 10: Merge to Historical
    if not execute_step(10, 10, "Merge to Historical", merge_main):
        sys.exit(1)
    
    # Calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    duration_str = str(duration).split('.')[0]  # Remove microseconds
    
    # Print for immediate dashboard output
    print("\n" + "="*80, flush=True)
    print("🎉 MANUAL PIPELINE EXECUTION COMPLETED SUCCESSFULLY!", flush=True)
    print(f"All 10 steps completed successfully.", flush=True)
    print(f"Total duration: {duration_str}", flush=True)
    print(f"Check logs/ folder for detailed logs", flush=True)
    print("="*80 + "\n", flush=True)
    
    # Log for file
    connection_string = f"mysql+pymysql://{config.MYSQL_USER}:{config.MYSQL_PASSWORD}@{config.MYSQL_HOST}/{config.INCREMENTAL_SCHEMA}"
    today = datetime.now().strftime("%Y-%m-%d")
    register_dump_in_queue(connection_string, today)

    log_header("🎉 MANUAL PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    log_info("All 10 steps completed successfully.")
    log_info(f"Total duration: {duration_str}")
    log_info(f"Check logs/ folder for detailed logs")
    
    # Send success notification
    if hasattr(config, 'NOTIFY_ON_SUCCESS') and config.NOTIFY_ON_SUCCESS:
        notifier.send_pipeline_success(
            duration=duration_str,
            stats={
                "Schema": config.INCREMENTAL_SCHEMA,
                "Steps": "10/10"
            }
        )
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
