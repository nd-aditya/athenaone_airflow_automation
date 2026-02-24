import os
from django.db import models
from django.conf import settings
from core.dbPkg.dbhandler import NDDBHandler
from .series import PatientSeries
from .utils import Status

class PatientInstance(models.Model):
    id = models.AutoField(primary_key=True)
    series = models.ForeignKey(PatientSeries, on_delete=models.CASCADE, related_name="instances")
    client_sop_instance_uid = models.CharField(max_length=80, null=True)
    nd_sop_instance_uid = models.CharField(max_length=80, null=True)
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
    original_file_path = models.TextField(default=None, null=True)
    failure_remarks = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"PatientInstance(id={self.id}, client-sop-uid={self.client_sop_instance_uid})"
    
    def get_deidentified_file_path(self):
        full_path = os.path.join(
            settings.PACS_DATA_SAVE_PATH,
            os.path.join(
                str(self.series.study.patient.nd_patient_id),
                os.path.join(
                    self.series.study.nd_study_instance_uid,
                    os.path.join(
                        self.series.nd_series_instance_uid,
                        f"{self.nd_sop_instance_uid}.dcm"
                    )
                )
            )
        )
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        return full_path

    def get_chain_reference_uuid_for_pacs_inventory(self):
        return f"patient_instance_{self.id}_inventory"