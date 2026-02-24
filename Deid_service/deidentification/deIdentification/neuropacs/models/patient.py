from django.db import models
from core.dbPkg.dbhandler import NDDBHandler
from .pacsclient import PacsClient
from .utils import Status


class Patients(models.Model):
    id = models.AutoField(primary_key=True)
    nd_patient_id = models.BigIntegerField(unique=True, null=True)
    client_patient_id = models.BigIntegerField(unique=True)
    offset_value = models.IntegerField(null=True)
    pacs_client = models.ForeignKey(PacsClient, on_delete=models.CASCADE, related_name="patients")
    deid_status = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not Started"),
            (Status.IN_PROGRESS, "In Progress"),
            (Status.COMPLETED, "Completed"),
            (Status.FAILED, "Failed"),
        ],
        default=Status.NOT_STARTED,
    )
    total_study_count = models.IntegerField(default=0)
    cloud_uploaded = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not Started"),
            (Status.IN_PROGRESS, "In Progress"),
            (Status.COMPLETED, "Completed"),
            (Status.FAILED, "Failed"),
        ],
        default=Status.NOT_STARTED,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Patients(id={self.id}, ndid={self.nd_patient_id}, pid={self.client_patient_id})"

    def get_chain_reference_uuid_for_pacs_inventory(self):
        return f"patient_{self.id}_inventory"