"""
When Airflow runs the deid pipeline, it writes a small override file so we use
diff_<date> and diff_<date>_deid without changing SchedulerConfig in the DB.
If the file is absent, all code uses SchedulerConfig as usual (UI path unchanged).
"""
import json
import os


def get_airflow_schema_override():
    """
    Read config/airflow_deid_override.json if it exists.
    Returns {"current_schema": str, "deid_schema": str} or None.
    """
    try:
        from django.conf import settings
        path = os.path.join(settings.BASE_DIR, "config", "airflow_deid_override.json")
        if os.path.isfile(path):
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, dict) and "current_schema" in data and "deid_schema" in data:
                return data
    except Exception:
        pass
    return None
