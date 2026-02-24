import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/rohitchouhan/Documents/Code/backend/deidentification/deIdentification"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from core.dbPkg.mapping_table.create_table import MappingTable


example_config = {
    "auto_generate_table": True,
    "connection_str": "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema_output",
    "patient_mapping_config": {
        "primary_id_column": "PATIENTID",
        "tables": [
            {
                "table_name": "table1",
                "columns": {
                    "PATIENTID": "patient_id",
                    "CHARTID": "chartid",
                },
            },
            {
                "table_name": "table2",
                "columns": {
                    "CHARTID": "chartid",
                    "PROFILEID": "profileid",
                },
            },
            {
                "table_name": "table4",
                "columns": {
                    "PROFILEID": "profileid",
                    "PID": "pid",
                },
            },
        ],
        "patient_identifier_columns": [
            "PATIENTID",
            "CHARTID",
            "PROFILEID",
            "PID"
        ],
        "ndid_start_value": 100101100001,
    },
    "encounter_mapping_config": {
        "table_name": "encounters",
        "encounter_id_column": "enc_id",
        "patient_identifier_column": "patient_id",
        "patient_identifier_type": "PATIENTID",
        "encounter_date_column": "registration_date",
    },
}

source_connection_str = "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema"
mapper = MappingTable(source_connection_str, example_config)
# mapper.generate_patient_mapping_table()  # creates/refreshes full table with timestamps
# mapper.update_patient_mapping_table()  # adds new rows, sets correct created_at/updated_at

mapper.generate_encounter_mapping_table()
