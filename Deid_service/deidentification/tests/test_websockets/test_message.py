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


from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
layer = get_channel_layer()
async_to_sync(layer.group_send)(
    "task_group",
    {"type": "send_task_status", "task_type": "deid_task", "table_name": "docdata", "task_status": "IN_PROGRESS"}
)
