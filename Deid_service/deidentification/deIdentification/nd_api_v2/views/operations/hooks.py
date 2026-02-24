from worker.models import Task, Chain
from nd_api.models.table_details import Table
from portal.alerts import SendMessage

# def nd_auto_increment_id_column_failure_hook(chain_obj: Chain):
#     pass

# def patient_mapping_generation_failure_hook(chain_obj: Chain):
#     pass


# def encounter_mapping_generation_failure_hook(chain_obj: Chain):
#     pass


# def master_table_generation_failure_hook(chain_obj: Chain):
#     pass

def de_identification_failure_hook_for_table(chain_obj: Chain):
    table_id = Table.get_table_id_from_chain_reference_uuid(
        chain_obj.reference_uuid
    )
    table_details_obj = Table.objects.get(id=table_id)
    table_details_obj.marked_as_failed()
    table_details_obj.save()

def qc_failure_hook_for_table(chain_obj: Chain):
    table_id = Table.get_table_id_from_chain_reference_uuid(
        chain_obj.reference_uuid
    )
    table_details_obj = Table.objects.get(id=table_id)
    table_details_obj.update_qc_status('failed')
    table_details_obj.save()