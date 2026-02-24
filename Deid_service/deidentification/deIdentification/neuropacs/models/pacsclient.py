from django.db import models
from core.dbPkg.dbhandler import NDDBHandler
from nd_api.models import Clients

class HandlerType:
    DIR_HANDLER = 1


class PacsClient(models.Model):
    id = models.AutoField(primary_key=True)
    client = models.ForeignKey(Clients, on_delete=models.CASCADE, related_name="pacs_client")
    handler_type = models.IntegerField(
        choices=[
            (HandlerType.DIR_HANDLER, "DIR_HANDLER"),
        ],
        default=None,
    )
    patient_identifier_type = models.CharField(max_length=100, default=None, null=True)
    run_config = models.JSONField(default=dict)
    register_date = models.DateField(default=None)
    inventory_creation_done = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"PacsClient(id={self.id}, register_date={self.register_date})"

    def get_chain_reference_uuid_for_pacs_inventory(self):
        return f"pacs_client_{self.id}_inventory"