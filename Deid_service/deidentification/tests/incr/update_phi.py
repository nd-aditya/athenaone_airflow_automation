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


from nd_api_v2.services.register_dump import register_dump_in_queue
from nd_api_v2.models.incremental_queue import IncrementalQueue
from nd_api_v2.models.table_details import TableMetadata

table_metadata = TableMetadata.objects.filter(table_name="encounters").last()
table_metadata.table_details_for_ui = {
    "batch_size": 1000,
    "ignore_rows": {},
    "columns_details": [
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "enc_id",
            "ignore_rows": {},
            "de_identification_rule": "ENCOUNTER_ID",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "patient_id",
            "ignore_rows": {},
            "de_identification_rule": "PATIENT_PATIENTID",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "registration_date",
            "ignore_rows": {},
            "de_identification_rule": "DATE_OFFSET",
            "column_name_for_phi_table": None,
        },
        {
            "is_phi": False,
            "mask_value": None,
            "column_name": "nd_auto_increment_id",
            "ignore_rows": {},
            "de_identification_rule": None,
            "column_name_for_phi_table": None,
        },
    ],
    "reference_mapping": {},
    "patient_identifier_type": None,
    "patient_identifier_column": None,
}
table_metadata.save()
