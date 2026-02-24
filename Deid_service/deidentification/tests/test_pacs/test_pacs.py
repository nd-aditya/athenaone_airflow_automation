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

from neuropacs.models import Patients

# Patients.objects.create(
#     nd_patient_id=100010101011,
#     client_patient_id=100,
#     offset_value=-30
# )

Patients.objects.all().delete()
