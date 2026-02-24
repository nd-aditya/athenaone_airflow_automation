import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/rohitchouhan/Documents/Code/backend/deidentification/deIdentification/"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from core.dbPkg.phi_table.create_table import PIITable



pii_tables_config = {
    "pii_schema_name": "dummy_master_schema_output",
    "pii_tables": {
        "pii_data_table": {
        "source_tables": {
            "table1": {
                "primary_column_name": "patient_id",
                "primary_column_type": "PATIENTID",
                "required_columns": [{'notes': 'Notes1'}],
            },
            "table2": {
                "primary_column_name": "chartid",
                "primary_column_type": "CHARTID",
                "required_columns": [{'dept': 'Dept1'}, {"notes2": "Notes1"}],
            }
        }
    }
    }
}
SRC_DB_URL = "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema"
DEST_DB_URL = "mysql+pymysql://root:123456789@localhost/dummy_master_schema_output"
MAPPING_DB_URL = "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema_output"
pii_mgr = PIITable(SRC_DB_URL, DEST_DB_URL, MAPPING_DB_URL, pii_tables_config)
pii_mgr.generate_or_update_pii_table()
