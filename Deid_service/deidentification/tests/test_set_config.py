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

from nd_api.models import Table

tableobj = Table.objects.get(table_name="encounters")
config = {
    "batch_size": 1000,
    "ignore_rows": {},
    "columns_details": [
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "enc_id",
            "ignore_rows": {},
            "reference_mapping": {},
            "de_identification_rule": "ENCOUNTER_ID",
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "patient_id",
            "ignore_rows": {},
            "reference_mapping": {},
            "de_identification_rule": "PATIENT_PATIENTID",
            
        },
        {
            "is_phi": True,
            "mask_value": None,
            "column_name": "registration_date",
            "ignore_rows": {},
            "reference_mapping": {},
            "de_identification_rule": "PATIENT_DOB",
            
        },
        {
            "is_phi": False,
            "mask_value": None,
            "column_name": "nd_auto_increment_id",
            "ignore_rows": {},
            "reference_mapping": {},
            "de_identification_rule": None,
            
        },
    ],
    "reference_mapping": {},
    "patient_identifier_column": "enc_id",
    "patient_identifier_type": "encounter_id",
}
tableobj.table_details_for_ui = config
tableobj.save()

