from django.db import models

class IgnoreRowsDeIdentificaiton(models.Model):
    id = models.AutoField(primary_key=True)
    queue_id = models.IntegerField()
    table_name = models.CharField(max_length=200)
    row = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"IgnoreRowsDeIdentificaiton(id={self.id}, {self.queue_id}, {self.table_name})"

