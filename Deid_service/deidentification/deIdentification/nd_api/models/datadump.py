from django.db import models
from typing import TypedDict
from core.dbPkg.dbhandler import NDDBHandler
from sqlalchemy.engine.url import make_url

from .client import Clients

class DumpDataStatus:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = -1

class DbStatsGeneratedStatus:
    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = -1


class MappingDbConfig(TypedDict, total=False):
    connection_str: str
    auto_generate_table: bool
    ndid_start_value: int
    mapping_query: str
    

class ClientDataDump(models.Model):
    id = models.AutoField(primary_key=True)
    dump_name = models.CharField(unique=True, max_length=200)
    source_db_config = models.JSONField(default=dict)
    run_config = models.JSONField(default=dict)

    # pii_config = {"column-name": '((mask-value))'}
    pii_config = models.JSONField(default=dict)

    # [{'table_name': "insurance_table", "config": {}}]
    secondary_config = models.JSONField(default=list)

    # global_config = [{'table_name': "insurance_table", "config": {}}]
    global_config = models.JSONField(default=list)
    qc_config = models.JSONField(default=dict)
    status  = models.IntegerField(
        choices=[
            (DumpDataStatus.NOT_STARTED, "Not Started"),
            (DumpDataStatus.IN_PROGRESS, "In Progress"),
            (DumpDataStatus.COMPLETED, "Completed"),
            (DumpDataStatus.FAILED, "Failed"),
        ],
        default=DumpDataStatus.NOT_STARTED,
    )
    stats_generated_status  = models.IntegerField(
        choices=[
            (DbStatsGeneratedStatus.NOT_STARTED, "Not Started"),
            (DbStatsGeneratedStatus.IN_PROGRESS, "In Progress"),
            (DbStatsGeneratedStatus.COMPLETED, "Completed"),
            (DbStatsGeneratedStatus.FAILED, "Failed"),
        ],
        default=DumpDataStatus.NOT_STARTED,
    )
    dump_stats = models.JSONField(default=dict)
    is_dump_processing_done = models.BooleanField(default=False)
    is_primary_key_uploaded = models.BooleanField(default=False)
    dump_date = models.DateTimeField(null=True)
    client = models.ForeignKey(
        Clients, on_delete=models.CASCADE, related_name="dumps"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"ClientDataDump(id={self.id})"

    def get_destination_dbname(self):
        dbname = f"deidentify_client_{self.client.id}_dump_{self.id}"
        return dbname
    
    def _get_db_url(self, schema_name: str):
        admin_conn_str = self.client.config['admin_connection_str']
        admin_url = make_url(admin_conn_str)
        db_url = admin_url.set(database=schema_name)
        admin_dbhandler = NDDBHandler(admin_conn_str)
        admin_dbhandler.create_database_if_not_exists(schema_name)
        return db_url.render_as_string(hide_password=False)
    
    def get_destination_db_connection_str(self):
        dest_db = self.get_destination_dbname()
        dest_url = self._get_db_url(dest_db)
        return dest_url

    def get_mapping_db_connection_str(self):
        mapping_schema = self.client.mapping_db_config["mapping_schema"]
        mapping_url = self._get_db_url(mapping_schema)
        return mapping_url

    def get_destination_db_connection(self) -> NDDBHandler:
        dest_conn_str = self.get_destination_db_connection_str()
        return NDDBHandler(dest_conn_str)
    
    def get_secondary_pii_configs(self):
        # {"connection_str": "", "tables_config": [{'table_name': "insurance_table", "config": {}}]
        schema_name = self.client.master_db_config["pii_schema_name"]
        conn_str = self._get_db_url(schema_name)
        pii_config = {"connection_str": conn_str, "tables_config": self.secondary_config}               
        return pii_config
    
    def get_pii_config(self):
        # {"connection_str": "", "config": {}}
        schema_name = self.client.master_db_config["pii_schema_name"]
        conn_str = self._get_db_url(schema_name)
        pii_config = {"connection_str": conn_str, "config": self.pii_config}
        return pii_config
    
    def get_global_config(self):
        raise NotImplemented("not implmented yet!!")
        # {"connection_str": "", "tables_config": [{'table_name': "insurance_table", "config": {}}]
        return self.global_config
    
    def get_mapping_db_config(self):
        nd_patinet_start_value = self.client.config['nd_patient_start_value']
        mapping_config: dict = self.client.mapping_db_config
        mapping_config['patient_mapping_config']['ndid_start_value'] = nd_patinet_start_value
        mapping_config['patient_mapping_config']['patient_identifier_columns'] = self.client.patient_identifier_columns
        mapping_config['connection_str'] = self.get_mapping_db_connection_str()
        return mapping_config
    
    def get_appointment_mapping_config(self):
        nd_patinet_start_value = self.client.config['nd_patient_start_value']
        appt_mapping_config: dict = self.client.mapping_db_config.get("appointment_mapping_config", {})
        appt_mapping_config['appointment_mapping_present'] = self.client.mapping_db_config.get("appointment_mapping_present", False)
        appt_mapping_config['auto_generate_table'] = self.client.mapping_db_config.get("auto_generate_table", True)
        appt_mapping_config['patient_mapping_config'] = {
            "ndid_start_value": nd_patinet_start_value,
            "patient_identifier_columns": self.client.patient_identifier_columns,
        }
        appt_mapping_config['connection_str'] = self.get_mapping_db_connection_str()
        appt_mapping_config['appointment_mapping_config'] = appt_mapping_config
        return appt_mapping_config

    def get_pii_schema_connection_str(self):
        pii_schema_name = self.client.master_db_config['pii_schema_name']
        return self._get_db_url(pii_schema_name)

    def is_auto_qc_enabled(self):
        return self.run_config.get("enable_auto_qc", False)
    
    def is_auto_gcp_upload_enabled(self):
        return self.run_config.get("enable_auto_gcp", False)

    def is_auto_embd_enabled(self):
        return self.run_config.get("enable_auto_embd", False)

    def get_source_db_connection(self) -> str:
        return NDDBHandler(self.source_db_config["connection_str"])
    
    def get_source_db_connection_str(self) -> NDDBHandler:
        return self.source_db_config["connection_str"]
    

    def marked_stats_generation_as_completed(self):
        self.stats_generated_status = DbStatsGeneratedStatus.COMPLETED
        self.save()

    def marked_stats_generation_as_failed(self):
        self.stats_generated_status = DbStatsGeneratedStatus.FAILED
        self.save()

    def get_admin_db_connection(self):
        return self.client.get_admin_db_connection()
    
    def get_chain_reference_uuid(self):
        return f"dump_{self.id}_deid"
    
    def get_chain_reference_uuid_for_bulk_deid(self):
        return f"dump_{self.id}_bulk_deid"

    def get_chain_reference_uuid_for_bulk_qc(self):
        return f"dump_{self.id}_bulk_qc"

    def get_chain_reference_uuid_for_bulk_embd(self):
        return f"dump_{self.id}_bulk_embd"

    def get_chain_reference_uuid_for_bulk_gcp(self):
        return f"dump_{self.id}_bulk_gcp"

    def get_chain_reference_uuid_for_bulk_deid_interrupt(self):
        return f"dump_{self.id}_bulk_deid_interrupt"

    def get_chain_reference_uuid_for_bulk_qc_interrupt(self):
        return f"dump_{self.id}_bulk_qc_interrupt"

    def get_chain_reference_uuid_for_bulk_embd_interrupt(self):
        return f"dump_{self.id}_bulk_embd_interrupt"

    def get_chain_reference_uuid_for_bulk_gcp_interrupt(self):
        return f"dump_{self.id}_bulk_gcp_interrupt"

# class RestoreDump(models.Model):
#     id = models.AutoField(primary_key=True)
#     dump = models.ForeignKey(
#         "DataDump", on_delete=models.CASCADE, related_name="restore_details"
#     )
#     config = models.JSONField()
#     status  = models.IntegerField(
#         choices=[
#             (DumpDataStatus.NOT_STARTED, "Not Started"),
#             (DumpDataStatus.IN_PROGRESS, "In Progress"),
#             (DumpDataStatus.COMPLETED, "Completed"),
#             (DumpDataStatus.FAILED, "Failed"),
#             (DumpDataStatus.INTERRUPTED, "Interrupted"),
#         ],
#         default=DumpDataStatus.NOT_STARTED,
#     )
#     created_at = models.DateTimeField(auto_now_add=True)
#     updated_at = models.DateTimeField(auto_now=True)

#     def __str__(self):
#         return f"RestoreDump(id={self.id}, dump={self.dump.id})"

