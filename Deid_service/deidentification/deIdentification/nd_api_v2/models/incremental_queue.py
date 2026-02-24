from django.db import models
from django.utils import timezone
from .table_details import Table, TableMetadata

class QueueStatus(models.IntegerChoices):
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = -1
    INTERUPTED = 3
    MANUALY_SKIPPED = 4


class IncrementalQueue(models.Model):
    id = models.AutoField(primary_key=True)
    queue_name = models.CharField(max_length=255)
    dump_date = models.DateField(null=False, default=timezone.now)
    queue_status = models.IntegerField(choices=QueueStatus.choices, default=QueueStatus.NOT_STARTED)
    

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"IncrementalQueue({self.queue_name} - {self.id})"
    
    def get_deid_chain_reference_uuid(self):
        return f"queue_deid_{self.id}"
    
    def get_destination_dbname(self):
        dbname = f"deidentify_client_{self.client.id}_dump_{self.id}"
        return dbname

    def get_chain_reference_uuid_for_bulk_qc(self):
        return f"qc_queue_bulk_{self.id}"