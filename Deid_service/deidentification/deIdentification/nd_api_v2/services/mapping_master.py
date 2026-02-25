import traceback
from worker.models import Task, Chain
from deIdentification.nd_logger import nd_logger
from core.dbPkg.mapping_table.create_table import MappingTable
from core.dbPkg.mapping_table.appointment import AppointmentMappingConfig, AppointmentMappingTable
from core.dbPkg.phi_table.create_table import PIITable
from deIdentification.nd_logger import nd_logger
from portal.alerts import alert_sender
from ndwebsocket.utils import broadcast_task_status, broadcast_task_error
from nd_api_v2.models.incremental_queue import IncrementalQueue
from nd_api_v2.models.table_details import Table
from nd_api_v2.models.configs import ClientRunConfig, get_mapping_db_connection_str, get_master_db_connection_str, MasterTableConfig, MappingConfig
from nd_api_v2.models.scheduler_config import SchedulerConfig


def run_patient_mapping_generation_task(queue_id: int, dependencies: list[Task] = []):
    try:
        incremental_queue = IncrementalQueue.objects.get(id=queue_id)
        client_run_config = ClientRunConfig.objects.last()
        scheduler_config = SchedulerConfig.objects.last()
        mapping_config_obj = MappingConfig.objects.last()
        if mapping_config_obj is None:
            raise Exception("Mapping configuration not found")
        mapping_config = mapping_config_obj.mapping_config
        if client_run_config is None:
            raise Exception("Client run configuration not found")
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")
        mapping_config['connection_str'] = get_mapping_db_connection_str()
        mapping_config['patient_mapping_config']['patient_identifier_columns'] = client_run_config.patient_identifier_columns
        mapping_config['patient_mapping_config']['ndid_start_value'] = client_run_config.nd_patient_start_value
        mapping_config['patient_mapping_config']['primary_id_column'] = mapping_config['patient_mapping_config']['primary_id_column']
        mptable = MappingTable(scheduler_config.get_historical_connection_str(), mapping_config, queue_id, client_run_config.ehr_type)
        mptable.update_patient_mapping_table()
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Patient Mapping: {incremental_queue.queue_name}",
            error=f"Patient mapping generation failed for queue ID {queue_id}: {str(e)}",
            error_code="PATIENT_MAPPING_ERROR",
            details={"queue_id": queue_id}
        )
        
        prepare_message = {
            "alert_type": "Patient Mapping Generation task Failed",
            "queue_identifier": f"{queue_id} - {incremental_queue.queue_name}",
            "traceback": traceback.format_exc()
        }
        #breakpoint()
        alert_sender.send_message(prepare_message)
        raise Exception(f"Raised issue because of {e}: trackeback:")

def run_encounter_mapping_generation_task(queue_id: int, dependencies: list[Task] = []):
    try:
        incremental_queue = IncrementalQueue.objects.get(id=queue_id)
        client_run_config = ClientRunConfig.objects.last()
        if client_run_config is None:
            raise Exception("Client run configuration not found")
        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")
        mapping_config_obj = MappingConfig.objects.last()
        if mapping_config_obj is None:
            raise Exception("Mapping configuration not found")
        mapping_config = mapping_config_obj.mapping_config
        mapping_config['connection_str'] = get_mapping_db_connection_str()
        mapping_config['patient_mapping_config']['patient_identifier_columns'] = client_run_config.patient_identifier_columns
        mapping_config['patient_mapping_config']['ndid_start_value'] = client_run_config.nd_patient_start_value
        mapping_config['patient_mapping_config']['primary_id_column'] = client_run_config.patient_identifier_columns[0]
        hist_conn = scheduler_config.get_historical_connection_str()
        # #region agent log
        try:
            import json
            _db = hist_conn.split("/")[-1].split("?")[0] if "/" in hist_conn else "unknown"
            with open("/Users/adityaneuroAI/athenaone_airflow_automation/.cursor/debug-2ecf71.log", "a") as f:
                f.write(json.dumps({"sessionId": "2ecf71", "location": "mapping_master.py:run_encounter_mapping", "message": "schema used for MappingTable source", "data": {"schema_from_connection": _db, "queue_id": queue_id}, "hypothesisId": "H2", "timestamp": __import__("datetime").datetime.utcnow().isoformat()}) + "\n")
        except Exception:
            pass
        # #endregion
        mptable = MappingTable(hist_conn, mapping_config, queue_id, client_run_config.ehr_type)
        mptable.generate_encounter_mapping_table()
    except Exception as e:
        # Broadcast error notification
        nd_logger.info(traceback.format_exc())
        broadcast_task_error(
            task_name=f"Encounter Mapping: {queue_id}",
            error=f"Encounter mapping generation failed for queue ID {queue_id}: {str(e)}",
            error_code="ENCOUNTER_MAPPING_ERROR",
            details={"queue_id": queue_id}
        )
        
        prepare_message = {
            "alert_type": "Ecnounter Mapping Generation task Failed",
            "queue_identifier": f"{queue_id} - {incremental_queue.queue_name}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)

def run_appointment_mapping_generation_task(queue_id: int, dependencies: list[Task] = []):
    try:
        incremental_queue = IncrementalQueue.objects.get(id=queue_id)
        tables = Table.objects.filter(incremental_queue=incremental_queue)
        client_run_config = ClientRunConfig.objects.last()
        if client_run_config is None:
            raise Exception("Client run configuration not found")
        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")
        for table in tables:
            appt_mapping_config = table.metadata.appointment_mapping_config
            mptable = AppointmentMappingTable(scheduler_config.get_historical_connection_str(), appt_mapping_config, queue_id, client_run_config.ehr_type)
            mptable.generate_appointment_mapping_table()
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Appointment Mapping: {incremental_queue.queue_name}",
            error=f"Appointment mapping generation failed for queue ID {queue_id}: {str(e)}",
            error_code="APPOINTMENT_MAPPING_ERROR",
            details={"queue_id": queue_id}
        )
        
        prepare_message = {
            "alert_type": "Appointment Mapping Generation task Failed",
            "queue_identifier": f"{queue_id} - {incremental_queue.queue_name}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)


def run_master_table_generation_task(queue_id: int, dependencies: list[Task] = []):
    try:
        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")
        src_db_url = scheduler_config.get_source_connection_str()

        master_db_url = get_master_db_connection_str()
        mapping_db_url = get_mapping_db_connection_str()
        pii_table_config = MasterTableConfig.objects.last().pii_tables_config
        pii = PIITable(src_db_url, master_db_url, mapping_db_url, pii_table_config['pii_tables'], queue_id)
        pii.generate_or_update_pii_table()

    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Master Table Generation: {queue_id}",
            error=f"Master table generation failed for dump ID {queue_id}: {str(e)}",
            error_code="MASTER_TABLE_ERROR",
            details={"queue_id": queue_id}
        )
        
        prepare_message = {
            "alert_type": "Master Table Generation task Failed",
            "queue_identifier": f"queue_id: {queue_id}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e
