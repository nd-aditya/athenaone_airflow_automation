from django.db import models
from .datadump import ClientDataDump
from deIdentification.nd_logger import nd_logger
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings


class Status:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = -1
    INTERUPTED = 3


class TableDEIDStatus(models.Model):
    id = models.AutoField(primary_key=True)
    failure_remarks = models.JSONField(default=dict)
    deid_status = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not Started"),
            (Status.IN_PROGRESS, "In Progress"),
            (Status.COMPLETED, "Completed"),
            (Status.FAILED, "Failed"),
            (Status.INTERUPTED, "Interrupted"),
        ],
        default=Status.NOT_STARTED,
    )
    
    def __str__(self):
        return f"TableDEIDStatus({self.table.table_name} - {self.id})"
    
    def marked_as_completed(self):
        self.deid_status = Status.COMPLETED
        self.save()
    
class TableQCStatus(models.Model):
    id = models.AutoField(primary_key=True)
    qc_status = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not Started"),
            (Status.IN_PROGRESS, "In Progress"),
            (Status.COMPLETED, "Completed"),
            (Status.FAILED, "Failed"),
            (Status.INTERUPTED, "Interrupted"),
        ],
        default=Status.NOT_STARTED,
    )
    qc_config = models.JSONField(default=dict)
    qc_result = models.JSONField(default=dict)

    def __str__(self):
        return f"TableQCStatus({self.table.table_name} - {self.id})"

class TableEmbeddingStatus(models.Model):
    id = models.AutoField(primary_key=True)
    embd_stats = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not Started"),
            (Status.IN_PROGRESS, "In Progress"),
            (Status.COMPLETED, "Completed"),
            (Status.FAILED, "Failed"),
            (Status.INTERUPTED, "Interrupted"),
        ],
        default=Status.NOT_STARTED,
    )
    failure_remarks = models.JSONField(default=dict)

    def __str__(self):
        return f"TableEmbeddingStatus({self.table.table_name} - {self.id})"

class TableGCPStatus(models.Model):
    id = models.AutoField(primary_key=True)
    cloud_uploaded = models.IntegerField(
        choices=[
            (Status.NOT_STARTED, "Not moved"),
            (Status.COMPLETED, "Moved"),
            (Status.FAILED, "Failed"),
            (Status.IN_PROGRESS, "In Process"),
            (Status.INTERUPTED, "Interrupted"),
        ],
        default=Status.NOT_STARTED,
    )
    md5sum = models.CharField(max_length=255, null=True, blank=True)
    failure_remarks = models.JSONField(default=dict)

    def __str__(self):
        return f"TableGCPStatus({self.table.table_name} - {self.id})"

class TableMetadata(models.Model):
    id = models.AutoField(primary_key=True)
    columns = models.JSONField(default=dict)
    primary_key = models.JSONField(default=dict)

    def __str__(self):
        return f"TableMetadata({self.table.table_name} - {self.id})"

class Table(models.Model):
    id = models.AutoField(primary_key=True)
    table_name = models.CharField(max_length=255, db_index=True)
    rows_count = models.IntegerField(null=True)
    dump = models.ForeignKey(
        ClientDataDump, on_delete=models.CASCADE, related_name="tables"
    )
    table_details_for_ui = models.JSONField(default=dict)
    metadata = models.OneToOneField(
        TableMetadata, on_delete=models.CASCADE, related_name="table"
    )
    deid = models.OneToOneField(
        TableDEIDStatus, on_delete=models.CASCADE, related_name="table"
    )
    qc = models.OneToOneField(
        TableQCStatus, on_delete=models.CASCADE, related_name="table"
    )
    gcp = models.OneToOneField(
        TableGCPStatus, on_delete=models.CASCADE, related_name="table"
    )
    embd = models.OneToOneField(
        TableEmbeddingStatus, on_delete=models.CASCADE, related_name="table"
    )
    is_phi_marking_done = models.BooleanField(default=False)
    is_phi_marking_locked = models.BooleanField(null=True)
    run_config = models.JSONField(default=dict)
    is_required = models.BooleanField(default=True)
    priority = models.IntegerField(default=0) # 1 : high, 2 : medium, 3 : low
    

    class Meta:
        unique_together = ("table_name", "dump")
        indexes = [
            models.Index(fields=["table_name", "dump"]),
        ]

    def __str__(self):
        return f"{self.table_name} - {self.dump.dump_name} - {self.id}"

    @classmethod
    def register_table(cls, table_name: str, dump: ClientDataDump, rerun: bool = False):
        if rerun:
            try:
                obj = cls.objects.get(table_name=table_name, dump=dump)
                return obj, False
            except cls.DoesNotExist:
                pass

        metadata = TableMetadata.objects.create()
        gcp = TableGCPStatus.objects.create()
        embd = TableEmbeddingStatus.objects.create()
        qc = TableQCStatus.objects.create()
        deid = TableDEIDStatus.objects.create()

        return cls.objects.get_or_create(
            table_name=table_name,
            dump=dump,
            defaults={
                "metadata": metadata,
                "gcp": gcp,
                "embd": embd,
                "qc": qc,
                "deid": deid,
            },
        )

    def get_deid_chain_reference_uuid(self):
        return f"db_{self.dump.id}_table_{self.id}"

    def get_qc_chain_reference_uuid(self):
        return f"qc_db_{self.dump.id}_table_{self.id}"

    @classmethod
    def get_table_id_from_chain_reference_uuid(cls, reference_uuid: str):
        try:
            return int(reference_uuid.split("_")[-1])
        except (IndexError, ValueError):
            return None

    def mark_as_in_progress_if_required(self):
        nd_logger.info("Marking table as in progress")
        self.deid.deid_status = Status.IN_PROGRESS
        self.deid.save()
        
        # Send structured table status update for UI
        from ndwebsocket.utils import broadcast_table_status_update
        broadcast_table_status_update(
            table_id=self.id,
            table_name=self.table_name,
            process_type='deid',
            status='in_progress',
            message=f"De-identification started for table {self.table_name}",
            save_to_db=False
        )


    def get_qc_config(self):
        if self.qc.qc_config == {}:
            return self.dump.qc_config
        return self.qc.qc_config
    
    def marked_as_completed(self):
        self.deid.marked_as_completed()
