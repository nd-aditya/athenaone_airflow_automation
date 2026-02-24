import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/ndaident/Desktop/DEPORTAL_INC/deidentification/deIdentification"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()


from nd_api_v2.services.mapping_master import run_patient_mapping_generation_task

run_patient_mapping_generation_task(14)