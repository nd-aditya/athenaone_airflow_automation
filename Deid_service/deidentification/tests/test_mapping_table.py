import os
import django
import sys
# Set up Django environment
sys.path.append('/Users/neurocenterne/Desktop/PORTAL/deidentification/deIdentification/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.models import DbDetailsModel, TableDetailsModel, IgnoreRowsDeIdentificaiton
from worker.models import Task, Chain
from django.contrib.auth.models import User
from keycloakauth.rolemodel import RoleModel, get_default_permissions
from nd_scripts.create_account import create_account
from nd_api.views.db_views import create_tasks_after_new_dump_registered
from core.process.main import start_de_identification_for_table
from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.views.db_views import run_stats_generation_task

def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()

from core.dbPkg.mapping_table.table import MappingTable
mapping_config = {
    # "source_connection_str": "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/mapping",
    # "dest_connection_str": "mysql+pymysql://ndadmin:ndADMIN%402025@localhost:3306/mapping",
    "dump_id": 1,
    "patient_start_value": 100100130000001,
    "mapping_query": "select u.uid AS client_patient_id, e.date  AS client_visit_date, e.encounterID AS client_encounter_id, u.register_date AS patient_registration_date from nddenttest.users2 as u left join nddenttest.enc_table2 as e on u.uid = e.patientID  where u.UserType = 3 order by 1, 2, 3"
}
mp = MappingTable(mapping_config)
mp.insert_data()