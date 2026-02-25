from django.db import models


def default_run_config():
    return {
        "source_connection_details": {
            "snowflake_user": "",
            "snowflake_password": "",
            "snowflake_account": "",
            "snowflake_database": "",
            "snowflake_warehouse": "",
            "snowflake_insecure_mode": True,
        },
        "destination_connection_details": {
            "mysql_user": "",
            "mysql_password": "",
            "mysql_host": "",
        },
        "schemas": {
            "incremental_schema": "dump_testing_date",
            "historical_schema": "athenaone",
        },
        "extraction_settings": {
            "context_ids": [1, 23649],
            "batch_size": 10000,
            "max_threads": 10,
            "test_tables": None,
        },
        "notification_settings": {
            "email_recipients": ["aalind@neurodiscovery.ai"],
            "google_chat_webhook": "",
            "enable_chat_notifications": False,
            "notify_on_start": True,
            "notify_on_step": True,
            "notify_on_success": True,
            "notify_on_failure": True,
        },
        "date_settings": {
            "extraction_date": None,
            "from_date": None,
            "to_date": None,
        },
        "machine_name": "Local",
    }


class SchedulerConfig(models.Model):
    id = models.AutoField(primary_key=True)
    run_config = models.JSONField(default=default_run_config)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"SchedulerConfig({self.id})"

    def get_source_connection_str(self):
        from nd_api_v2.airflow_override import get_airflow_schema_override
        override = get_airflow_schema_override()
        if "destination_connection_details" in self.run_config:
            connection_details = self.run_config['destination_connection_details']
            schema = (override.get("current_schema") if override else None) or self.run_config['schemas'].get('current_schema')
            connection_string = f"mysql+pymysql://{connection_details['mysql_user']}:{connection_details['mysql_password']}@{connection_details['mysql_host']}/{schema}"
        else:
            connection_details = self.run_config['main_database_details']
            schema = (override.get("current_schema") if override else None) or connection_details.get('database_name')
            connection_string = f"mysql+pymysql://{connection_details['username']}:{connection_details['password']}@{connection_details['host']}/{schema}"
        return connection_string

    def get_deid_connection_str(self):
        from nd_api_v2.airflow_override import get_airflow_schema_override
        override = get_airflow_schema_override()
        connection_details = self.run_config['deid_connection_details']
        schema = (override.get("deid_schema") if override else None) or connection_details.get('schema')
        connection_string = f"mysql+pymysql://{connection_details['mysql_user']}:{connection_details['mysql_password']}@{connection_details['mysql_host']}/{schema}"
        return connection_string
    
    def get_bridge_db_connection_str(self):
        connection_details = self.run_config['bridgedb_connection_details']
        connection_string = f"mysql+pymysql://{connection_details['mysql_user']}:{connection_details['mysql_password']}@{connection_details['mysql_host']}/{connection_details['schema']}"
        return connection_string
    
    def get_historical_connection_str(self):
        if "destination_connection_details" in self.run_config:
            connection_details = self.run_config['destination_connection_details']
            schema = self.run_config['schemas']['historical_schema']
            connection_string = f"mysql+pymysql://{connection_details['mysql_user']}:{connection_details['mysql_password']}@{connection_details['mysql_host']}/{schema}"
        else:
            connection_details = self.run_config['main_database_details']
            schema = connection_details['database_name']
            connection_string = f"mysql+pymysql://{connection_details['username']}:{connection_details['password']}@{connection_details['host']}/{schema}"
        return connection_string