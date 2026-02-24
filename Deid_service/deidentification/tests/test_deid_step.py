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
import requests
from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.models import Clients, ClientDataDump, Table, IgnoreRowsDeIdentificaiton
from worker.models import Task, Chain
from django.contrib.auth.models import User
from keycloakauth.rolemodel import RoleModel, get_default_permissions
from nd_scripts.create_account import create_account
from nd_api.views.db_views import create_tasks_after_new_dump_registered
from core.process.main import start_de_identification_for_table
from nd_api.views.de_identification_task import create_deidentification_task
from nd_api.views.db_views import run_stats_generation_task
from django.urls import reverse


def clean_db():
    RoleModel.objects.all().delete()
    User.objects.all().delete()
    DbDetailsModel.objects.all().delete()
    Chain.objects.all().delete()


def test_deid_state():
    client_obj = Clients.objects.last()
    client_obj.config['admin_connection_str'] = 'mysql+mysqlconnector://root:123456789@localhost/nddenttest'
    client_obj.save()
    dump_obj = client_obj.dumps.last()
    dump_obj.run_config['enc_to_pid_column_map'] = "PATIENTID"
    dump_obj.save()
    # Use full URL instead of reverse() directly
    base_url = (
        "http://127.0.0.1:8000"  # Adjust if your server runs on another host/port
    )
    endpoint = reverse("start_whole_identification", kwargs={"dump_id": client_obj.id})
    url = f"{base_url}{endpoint}"

    response = requests.get(url)
    print(response.status_code)
    print(response.json())

Chain.objects.all().delete()
test_deid_state()
