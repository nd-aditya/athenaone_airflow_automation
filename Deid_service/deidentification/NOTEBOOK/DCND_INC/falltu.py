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


# from nd_api_v2.services.register_dump import register_dump_in_queue
# from nd_api_v2.models.incremental_queue import IncrementalQueue
# from nd_api_v2.models import IgnoreRowsDeIdentificaiton
# from nd_api_v2.models.table_details import TableMetadata,Table
# from nd_api_v2.services.mapping_master import run_encounter_mapping_generation_task
# from nd_api_v2.services.mapping_master import run_patient_mapping_generation_task
# from nd_api_v2.services.mapping_master import generate_patient_mapping_table
from nd_api_v2.services.deid import create_or_update_bridge_table
from core.dbPkg import NDDBHandler

create_or_update_bridge_table(8528)