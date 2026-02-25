#!/usr/bin/env python3
"""
Config Loader - Loads configuration from Django SchedulerConfig model
Provides a flat config interface compatible with existing scripts
"""

import os
import sys
from pathlib import Path


def load_config_from_model():
    """Load configuration from Django SchedulerConfig model"""
    try:
        from nd_api_v2.models.scheduler_config import SchedulerConfig
        
        # Get the latest config
        scheduler_config = SchedulerConfig.objects.last()
        
        if scheduler_config is None:
            # Use default config if no config exists
            from nd_api_v2.models.scheduler_config import default_run_config
            run_config = default_run_config()
        else:
            run_config = scheduler_config.run_config
        
        return run_config
    except Exception as e:
        print(f"Warning: Could not load config from model: {e}")
        return None


def get_config():
    """
    Get configuration as a flat dictionary compatible with existing scripts
    Maps nested Django model structure to flat config variables
    """
    # Default fallback values
    default_config = {
        # Snowflake Connection
        'SNOWFLAKE_USER': 'sgarai',
        'SNOWFLAKE_PASSWORD': 'NDAI%40athenaone1234',
        'SNOWFLAKE_ACCOUNT': 'CM97887-READER_DATAPOND_DLSE_PROD_NEURO_DISCOVERY_AI',
        'SNOWFLAKE_DATABASE': 'ATHENAHEALTH',
        'SNOWFLAKE_WAREHOUSE': 'AH_WAREHOUSE',
        'SNOWFLAKE_INSECURE_MODE': True,
        
        # MySQL Connection
        'MYSQL_USER': 'nd-siddharth',
        'MYSQL_PASSWORD': 'ndSID%402025',
        'MYSQL_HOST': '172.16.2.42',
        
        # Schemas
        'INCREMENTAL_SCHEMA': 'dump_testing',
        'HISTORICAL_SCHEMA': 'athenaone',
        
        # Extraction Settings
        'CONTEXT_IDS': (1, 23649),
        'BATCH_SIZE': 10000,
        'MAX_THREADS': 10,
        'TEST_TABLES': None,
        
        # Date settings
        'EXTRACTION_DATE': None,
        'FROM_DATE': None,
        'TO_DATE': None,
        
        # Notification Settings
        'EMAIL_RECIPIENTS': ['aalind@neurodiscovery.ai'],
        'GOOGLE_CHAT_WEBHOOK': '',
        'ENABLE_CHAT_NOTIFICATIONS': False,
        'NOTIFY_ON_START': True,
        'NOTIFY_ON_STEP': True,
        'NOTIFY_ON_SUCCESS': True,
        'NOTIFY_ON_FAILURE': True,
        
        # Machine Identification
        'MACHINE_NAME': 'Local',
    }
    
    # Try to load from Django model (Django should already be set up when called from views)
    try:
        run_config = load_config_from_model()
        
        if run_config:
            # Map nested structure to flat config
            source = run_config.get('source_connection_details', {})
            dest = run_config.get('destination_connection_details', {})
            notifications = run_config.get('notification_settings', {})
            dates = run_config.get('date_settings', {})
            
            # Update config from model
            if source:
                default_config['SNOWFLAKE_USER'] = source.get('snowflake_user', default_config['SNOWFLAKE_USER'])
                default_config['SNOWFLAKE_PASSWORD'] = source.get('snowflake_password', default_config['SNOWFLAKE_PASSWORD'])
                default_config['SNOWFLAKE_ACCOUNT'] = source.get('snowflake_account', default_config['SNOWFLAKE_ACCOUNT'])
                default_config['SNOWFLAKE_DATABASE'] = source.get('snowflake_database', default_config['SNOWFLAKE_DATABASE'])
                default_config['SNOWFLAKE_WAREHOUSE'] = source.get('snowflake_warehouse', default_config['SNOWFLAKE_WAREHOUSE'])
                default_config['SNOWFLAKE_INSECURE_MODE'] = source.get('snowflake_insecure_mode', default_config['SNOWFLAKE_INSECURE_MODE'])
            
            if dest:
                default_config['MYSQL_USER'] = dest.get('mysql_user', default_config['MYSQL_USER'])
                default_config['MYSQL_PASSWORD'] = dest.get('mysql_password', default_config['MYSQL_PASSWORD'])
                default_config['MYSQL_HOST'] = dest.get('mysql_host', default_config['MYSQL_HOST'])
            
            if notifications:
                default_config['EMAIL_RECIPIENTS'] = notifications.get('email_recipients', default_config['EMAIL_RECIPIENTS'])
                default_config['GOOGLE_CHAT_WEBHOOK'] = notifications.get('google_chat_webhook', default_config['GOOGLE_CHAT_WEBHOOK'])
                default_config['ENABLE_CHAT_NOTIFICATIONS'] = notifications.get('enable_chat_notifications', default_config['ENABLE_CHAT_NOTIFICATIONS'])
                default_config['NOTIFY_ON_START'] = notifications.get('notify_on_start', default_config['NOTIFY_ON_START'])
                default_config['NOTIFY_ON_STEP'] = notifications.get('notify_on_step', default_config['NOTIFY_ON_STEP'])
                default_config['NOTIFY_ON_SUCCESS'] = notifications.get('notify_on_success', default_config['NOTIFY_ON_SUCCESS'])
                default_config['NOTIFY_ON_FAILURE'] = notifications.get('notify_on_failure', default_config['NOTIFY_ON_FAILURE'])
            
            if dates:
                default_config['EXTRACTION_DATE'] = dates.get('extraction_date', default_config['EXTRACTION_DATE'])
                default_config['FROM_DATE'] = dates.get('from_date', default_config['FROM_DATE'])
                default_config['TO_DATE'] = dates.get('to_date', default_config['TO_DATE'])
            
            # Update schemas
            schemas = run_config.get('schemas', {})
            if schemas:
                default_config['INCREMENTAL_SCHEMA'] = schemas.get('incremental_schema', default_config['INCREMENTAL_SCHEMA'])
                default_config['HISTORICAL_SCHEMA'] = schemas.get('historical_schema', default_config['HISTORICAL_SCHEMA'])
            # Airflow override: when running from Airflow, use diff_<date> as current schema
            try:
                from nd_api_v2.airflow_override import get_airflow_schema_override
                airflow_override = get_airflow_schema_override()
                if airflow_override and airflow_override.get('current_schema'):
                    default_config['INCREMENTAL_SCHEMA'] = airflow_override['current_schema']
            except Exception:
                pass
            
            # Update extraction settings
            extraction = run_config.get('extraction_settings', {})
            if extraction:
                context_ids = extraction.get('context_ids', None)
                if context_ids and isinstance(context_ids, list) and len(context_ids) == 2:
                    default_config['CONTEXT_IDS'] = tuple(context_ids)
                default_config['BATCH_SIZE'] = extraction.get('batch_size', default_config['BATCH_SIZE'])
                default_config['MAX_THREADS'] = extraction.get('max_threads', default_config['MAX_THREADS'])
                default_config['TEST_TABLES'] = extraction.get('test_tables', default_config['TEST_TABLES'])
            
            if 'machine_name' in run_config:
                default_config['MACHINE_NAME'] = run_config.get('machine_name', default_config['MACHINE_NAME'])
    except Exception as e:
        # Django not set up or model not available - use defaults
        # This is fine when running as standalone script (though not recommended)
        pass
    
    return default_config


# Create a config module-like object
class Config:
    """Config class that mimics the old config.py module"""
    
    def __init__(self):
        config_dict = get_config()
        for key, value in config_dict.items():
            setattr(self, key, value)


# Create a singleton instance
_config_instance = None

def get_config_instance():
    """Get or create the config instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance


# For backward compatibility, create module-level variables
_config = get_config_instance()

# Export all config variables as module attributes
SNOWFLAKE_USER = _config.SNOWFLAKE_USER
SNOWFLAKE_PASSWORD = _config.SNOWFLAKE_PASSWORD
SNOWFLAKE_ACCOUNT = _config.SNOWFLAKE_ACCOUNT
SNOWFLAKE_DATABASE = _config.SNOWFLAKE_DATABASE
SNOWFLAKE_WAREHOUSE = _config.SNOWFLAKE_WAREHOUSE
SNOWFLAKE_INSECURE_MODE = _config.SNOWFLAKE_INSECURE_MODE

MYSQL_USER = _config.MYSQL_USER
MYSQL_PASSWORD = _config.MYSQL_PASSWORD
MYSQL_HOST = _config.MYSQL_HOST

INCREMENTAL_SCHEMA = _config.INCREMENTAL_SCHEMA
HISTORICAL_SCHEMA = _config.HISTORICAL_SCHEMA

CONTEXT_IDS = _config.CONTEXT_IDS
BATCH_SIZE = _config.BATCH_SIZE
MAX_THREADS = _config.MAX_THREADS
TEST_TABLES = _config.TEST_TABLES

EXTRACTION_DATE = _config.EXTRACTION_DATE
FROM_DATE = _config.FROM_DATE
TO_DATE = _config.TO_DATE

EMAIL_RECIPIENTS = _config.EMAIL_RECIPIENTS
GOOGLE_CHAT_WEBHOOK = _config.GOOGLE_CHAT_WEBHOOK
ENABLE_CHAT_NOTIFICATIONS = _config.ENABLE_CHAT_NOTIFICATIONS
NOTIFY_ON_START = _config.NOTIFY_ON_START
NOTIFY_ON_STEP = _config.NOTIFY_ON_STEP
NOTIFY_ON_SUCCESS = _config.NOTIFY_ON_SUCCESS
NOTIFY_ON_FAILURE = _config.NOTIFY_ON_FAILURE

MACHINE_NAME = _config.MACHINE_NAME

