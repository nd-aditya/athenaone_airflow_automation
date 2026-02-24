from django.db.models.signals import post_save
from django.dispatch import receiver
from .table_details import (
    Table, TableDEIDStatus, TableQCStatus, TableGCPStatus,
    TableEmbeddingStatus, TableMetadata
)

@receiver(post_save, sender=Table)
def create_related_status_models(sender, instance, created, **kwargs):
    if created:
        # Create and link related models
        deid = TableDEIDStatus.objects.create()
        qc = TableQCStatus.objects.create()
        gcp = TableGCPStatus.objects.create()
        embd = TableEmbeddingStatus.objects.create()
        metadata = TableMetadata.objects.create()

        # Set them on the table and save
        instance.deid = deid
        instance.qc = qc
        instance.gcp = gcp
        instance.embd = embd
        instance.metadata = metadata
        instance.save()
