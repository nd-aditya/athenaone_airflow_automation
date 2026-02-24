from django.db import models
from core.dbPkg.dbhandler import NDDBHandler
from sqlalchemy.engine.url import make_url


class Clients(models.Model):
    id = models.AutoField(primary_key=True)
    client_name = models.CharField(unique=True, max_length=200)
    emr_type = models.CharField(max_length=200)

    # config = {'admin_connection_str': '', 'patient_identifier_columns': ["PATIENTID", "CHARTID", "PROFILEID", "PID"], 'nd_patient_start_value': 1001101011, 'default_offset_value': 34}
    config = models.JSONField(default=dict)
    mapping_db_config = models.JSONField(default=dict)

    # master_db_config = {
    #     "connection_str": "",
    #     "tables": {
    #         "pii_data_table": {
    #             "primary_column_name": "patient_id",
    #             "upsert_instead_of_append": True,
    #             "tables": {
    #                 "PATIENT": {
    #                     "primary_col": "PATIENTID",
    #                     "other_required_columns": ["CONTEXTID", "CONTEXTNAME", "ENTERPRISEID", "FIRSTNAME", "LASTNAME", "MIDDLEINITIAL"]
    #                 }
    #             }
    #         }
    #     }
    # }
    master_db_config = models.JSONField(default=dict)
    client_presetup_config_configured = models.BooleanField(default=False)
    presetup_remarks = models.JSONField(default=dict)
    patient_identifier_columns = models.JSONField(default=list)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Clients(id={self.id}, name={self.client_name}, emr={self.emr_type})"

    def _get_db_url(self, schema_name: str):
        admin_conn_str = self.config['admin_connection_str']
        admin_url = make_url(admin_conn_str)
        db_url = admin_url.set(database=schema_name)
        admin_dbhandler = NDDBHandler(admin_conn_str)
        admin_dbhandler.create_database_if_not_exists(schema_name)
        return db_url.render_as_string(hide_password=False)
    
    def get_mapping_db_connection_str(self):
        mapping_schema = self.mapping_db_config["mapping_schema"]
        mapping_url = self._get_db_url(mapping_schema)
        return mapping_url

    def get_mapping_db_config(self):
        nd_patinet_start_value = self.config['nd_patient_start_value']
        mapping_config: dict = self.mapping_db_config
        mapping_config['patient_mapping_config']['ndid_start_value'] = nd_patinet_start_value
        mapping_config['patient_mapping_config']['patient_identifier_columns'] = self.patient_identifier_columns
        mapping_config['connection_str'] = self.get_mapping_db_connection_str()
    
    def get_admin_db_connection(self):
        return NDDBHandler(self.config["admin_connection_str"])

    def get_reference_mapping_connection_str(self):
        return self.config["referene_mapping_connection_str"]