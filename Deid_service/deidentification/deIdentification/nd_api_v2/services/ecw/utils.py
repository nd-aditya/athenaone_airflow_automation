import os
import json
from django.conf import settings

AUTO_INCREMENT_COL = "nd_auto_increment_id"
DIFF_CREATED_DATE_COL = "nd_extracted_date"
EXTRA_COLUMNS = [AUTO_INCREMENT_COL, DIFF_CREATED_DATE_COL]


def get_incremental_diff_database_name(config: dict):
    if "database" in config['incremental_diff_database_details']:
        return config['incremental_diff_database_details']['database']
    client_name = config['client_name']
    dump_date = config['dump_date'].replace("-", "")
    return f"diff_{client_name}_{dump_date}"


def get_find_incremental_logs_path(config: dict):
    return os.path.join(settings.LOGS_DIR, f"ecw_find_incremental_{config['client_name']}_{config['dump_date']}.json")

def load_find_incremental_logs(config: dict):
    if os.path.exists(get_find_incremental_logs_path(config)) and os.path.getsize(get_find_incremental_logs_path(config)) > 0:
        with open(get_find_incremental_logs_path(config), "r") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load find incremental logs: {e}")
    return {}

def save_find_incremental_logs(logs: dict, config: dict):
    with open(get_find_incremental_logs_path(config), "w") as f:
        json.dump(logs, f, indent=2)

def get_merge_incremental_diff_logs_path(config: dict):
    return os.path.join(settings.LOGS_DIR, f"ecw_merge_incremental_diff_{config['client_name']}_{config['dump_date']}.json")

def load_merge_incremental_diff_logs(config: dict = None):
    if config is None:
        # Try to find the most recent config-based log file
        log_dir = settings.LOGS_DIR
        if os.path.exists(log_dir):
            log_files = [f for f in os.listdir(log_dir) if f.startswith("ecw_merge_incremental_diff_") and f.endswith(".json")]
            if log_files:
                # Use most recently modified file
                latest_file = max(log_files, key=lambda f: os.path.getmtime(os.path.join(log_dir, f)))
                log_path = os.path.join(log_dir, latest_file)
                if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
                    with open(log_path, "r") as f:
                        try:
                            return json.load(f)
                        except Exception as e:
                            print(f"Warning: Failed to load merge incremental diff logs: {e}")
        return {}
    
    if os.path.exists(get_merge_incremental_diff_logs_path(config)) and os.path.getsize(get_merge_incremental_diff_logs_path(config)) > 0:
        with open(get_merge_incremental_diff_logs_path(config), "r") as f:
            try:
                return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load merge incremental diff logs: {e}")
    return {}

def save_merge_incremental_diff_logs(logs: dict, config: dict):
    with open(get_merge_incremental_diff_logs_path(config), "w") as f:
        json.dump(logs, f, indent=2)