from django.db import models
from core.dbPkg.dbhandler import NDDBHandler
from .study import PatientStudy
from .utils import Status

class PatientSeries(models.Model):
    id = models.AutoField(primary_key=True)
    client_series_instance_uid = models.CharField(max_length=80, null=True)
    study = models.ForeignKey(PatientStudy, on_delete=models.CASCADE, related_name="series")
    nd_series_instance_uid = models.CharField(max_length=80, null=True)
    total_instances_count = models.IntegerField(default=0)
    deid_status = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not Started"),
            (Status.IN_PROGRESS, "In Progress"),
            (Status.COMPLETED, "Completed"),
            (Status.FAILED, "Failed"),
        ],
        default=Status.NOT_STARTED,
    )
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
        return f"PatientSeries(id={self.id}, client-series-uid={self.client_series_instance_uid})"

    def get_chain_reference_uuid_for_pacs_inventory(self):
        return f"patient_series_{self.id}_inventory"