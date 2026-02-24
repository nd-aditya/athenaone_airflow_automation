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


from nd_api_v2.services.register_dump import register_dump_in_queue
from nd_api_v2.models.incremental_queue import IncrementalQueue
from nd_api_v2.models.table_details import TableMetadata

IncrementalQueue.objects.all().delete()
#TableMetadata.objects.all().delete()


#connection_string = "mysql+pymysql://root:123456789@localhost/daily_dump"
connection_string = "mysql+pymysql://ndadmin:ndADMIN%402025@localhost/differential_data"
dump_date = "2026-02-18"
register_dump_in_queue(connection_string, dump_date)