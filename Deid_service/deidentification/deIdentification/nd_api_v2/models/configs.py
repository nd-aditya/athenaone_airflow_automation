from django.db import models
from django.db import models
from typing import TypedDict
from core.dbPkg.dbhandler import NDDBHandler
from sqlalchemy.engine.url import make_url


EHR_TYPE_CHOICES = [
    ('AnthenaOne', 'anthenaone'),
    ('eCW', 'ecw'),
    ('AthenPractice', 'athenpractice'),
]

class MappingConfig(models.Model):
    id = models.AutoField(primary_key=True)
    is_configured = models.BooleanField(default=False)
    mapping_config = models.JSONField(default=dict)
    mapping_schema = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"MappingConfig({self.id})"


class MasterTableConfig(models.Model):
    id = models.AutoField(primary_key=True)
    is_configured = models.BooleanField(default=False)
    pii_tables_config = models.JSONField(default=dict)
    pii_schema_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"MasterTableConfig({self.id})"
    

class PIIMaskingConfig(models.Model):
    id = models.AutoField(primary_key=True)
    is_configured = models.BooleanField(default=False)
    pii_masking_config = models.JSONField(default=dict)
    secondary_config = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"PIIMaskingConfig({self.id})"

class QCConfig(models.Model):   
    id = models.AutoField(primary_key=True)
    is_configured = models.BooleanField(default=False)
    qc_config = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"QCConfig({self.id})"

class ClientRunConfig(models.Model):
    id = models.AutoField(primary_key=True)
    is_configured = models.BooleanField(default=False)
    patient_identifier_columns = models.JSONField(default=list)
    admin_connection_str = models.CharField(max_length=255)
    nd_patient_start_value = models.BigIntegerField(default=0)
    default_offset_value = models.IntegerField(default=0)
    enable_auto_qc = models.BooleanField(default=False)
    enable_auto_gcp = models.BooleanField(default=False)
    enable_auto_embd = models.BooleanField(default=False)
    ehr_type = models.CharField(max_length=255, choices=EHR_TYPE_CHOICES, default='AnthenaOne')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"ClientRunConfig({self.id})"
    
    
def _get_db_url(schema_name: str):
    client_run_config = ClientRunConfig.objects.last()
    if client_run_config is None:
        raise Exception("Client run configuration not found")
    admin_conn_str = client_run_config.admin_connection_str
    admin_url = make_url(admin_conn_str)
    db_url = admin_url.set(database=schema_name)
    admin_dbhandler = NDDBHandler(admin_conn_str)
    admin_dbhandler.create_database_if_not_exists(schema_name)
    return db_url.render_as_string(hide_password=False)
    
def get_mapping_db_connection_str():
    mapping_config = MappingConfig.objects.last()
    if mapping_config is None:
        raise Exception("Mapping configuration not found")
    mapping_schema = mapping_config.mapping_schema
    mapping_url = _get_db_url(mapping_schema)
    return mapping_url

def get_master_db_connection_str():
    master_config = MasterTableConfig.objects.last()
    if master_config is None:
        raise Exception("Master table configuration not found")
    master_schema = master_config.pii_schema_name
    master_url = _get_db_url(master_schema)
    return master_url


def get_mapping_table_config():
    mapping_config_obj = MappingConfig.objects.last()
    if mapping_config_obj is None:
        raise Exception("Mapping configuration not found")
    mapping_config = mapping_config_obj.mapping_config
    mapping_config['connection_str'] = get_mapping_db_connection_str()
    return mapping_config

def get_pii_config():
    pii_mask_obj = PIIMaskingConfig.objects.last()
    if pii_mask_obj is None:
        raise Exception("PII masking configuration not found")
    pii_mask_config = pii_mask_obj.pii_masking_config
    pii_mask_config['connection_str'] = get_master_db_connection_str()
    return pii_mask_config

def get_secondary_pii_configs():
    pii_mask_obj = PIIMaskingConfig.objects.last()
    if pii_mask_obj is None:
        raise Exception("Secondary PII configuration not found")
    return pii_mask_obj.secondary_config

def is_auto_qc_enabled():
    client_run_config = ClientRunConfig.objects.last()
    if client_run_config is None:
        raise Exception("Client run configuration not found")
    return client_run_config.enable_auto_qc

def is_auto_gcp_enabled():
    client_run_config = ClientRunConfig.objects.last()
    if client_run_config is None:
        raise Exception("Client run configuration not found")
    return client_run_config.enable_auto_gcp

def is_auto_embd_enabled():
    client_run_config = ClientRunConfig.objects.last()
    if client_run_config is None:
        raise Exception("Client run configuration not found")
    return client_run_config.enable_auto_embd