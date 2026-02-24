from django.db import transaction
from deIdentification.settings import CREATE_SAVEPOINT_IN_TRANSACTION
from nd_api_v2.models.incremental_queue import IncrementalQueue, QueueStatus
from nd_api_v2.models.table_details import Table, Status
from worker.models import Chain, Task
from ndwebsocket.utils import broadcast_task_status
from ndwebsocket.models import NotificationPriority, NotificationType
from .mapping_master import run_patient_mapping_generation_task, run_encounter_mapping_generation_task, run_appointment_mapping_generation_task, run_master_table_generation_task
from .deid import create_deidentification_task
from deIdentification.nd_logger import nd_logger
from django.conf import settings


def process_incremental_queue(incremental_queue: IncrementalQueue):
    broadcast_task_status(
        status="started",
        task_name=f"Queue Processing: {incremental_queue.queue_name}",
        message=f"Starting dump processing for {incremental_queue.queue_name}",
        priority=NotificationPriority.HIGH,
        notification_type=NotificationType.TASK_STATUS,
        data={"queue_id": incremental_queue.id}
    )
    nd_logger.info(f"Queue {incremental_queue.queue_name} processing started")
    with transaction.atomic(savepoint=CREATE_SAVEPOINT_IN_TRANSACTION):
        chain, created = Chain.all_objects.get_or_create(
            reference_uuid=incremental_queue.get_deid_chain_reference_uuid()
        )
        if not created:
            chain.revive_and_save()
        nd_logger.info(f"Chain {chain.reference_uuid} created or revived")
        tasks = []
        patient_mapping_task = Task.create_task(
            chain=chain,
            fn=run_patient_mapping_generation_task,
            arguments={"queue_id": incremental_queue.id},
            dependencies=[],
        )
        tasks.append(patient_mapping_task)
        enc_mapping_task = Task.create_task(
            chain=chain,
            fn=run_encounter_mapping_generation_task,
            arguments={"queue_id": incremental_queue.id},
            dependencies=[patient_mapping_task],
        )
        tasks.append(enc_mapping_task)
        appt_mapping_task = Task.create_task(
            chain=chain,
            fn=run_appointment_mapping_generation_task,
            arguments={"queue_id": incremental_queue.id},
            dependencies=[patient_mapping_task],
        )
        tasks.append(appt_mapping_task)
        master_table_task = Task.create_task(
            chain=chain,
            fn=run_master_table_generation_task,
            arguments={"queue_id": incremental_queue.id},
            dependencies=[patient_mapping_task],
        )
        tasks.append(master_table_task)
        
        Task.create_task(
            fn=create_deidentification_tasks_for_all_tables,
            chain=chain,
            dependencies=tasks,
            arguments={"queue_id": incremental_queue.id}
        )
        nd_logger.info(f"Queue {incremental_queue.queue_name} processing started")


def create_deidentification_tasks_for_all_tables(queue_id: int, dependencies: list[Task] = []):
    incremental_queue = IncrementalQueue.objects.get(id=queue_id)
    tables = Table.objects.filter(incremental_queue=incremental_queue)
    chain, created = Chain.all_objects.get_or_create(
        reference_uuid=incremental_queue.get_deid_chain_reference_uuid()
    )
    if not created:
        chain.revive_and_save()
    tasks_list = []
    for table in tables:
        tasks, _chain = create_deidentification_task(table, chain)
        tasks_list.extend(tasks)
    
    Task.create_task(
        fn=mark_queue_completed,
        chain=chain,
        dependencies=tasks_list,
        arguments={"queue_id": incremental_queue.id, "chain_id": chain.id}
    )
    return tasks_list


def mark_queue_completed(queue_id: int, chain_id: int, dependencies: list[Task] = []):
    incremental_queue  = IncrementalQueue.objects.get(id=queue_id)
    incremental_queue.queue_status = QueueStatus.COMPLETED
    incremental_queue.save()
    chain = Chain.objects.get(id=chain_id)
    with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
        chain.soft_delete_and_save()
    nd_logger.info(f"Queue {incremental_queue.queue_name} completed")
    broadcast_task_status(
        status="completed",
        task_name=f"Queue Completion: {incremental_queue.queue_name}",
        message=f"Queue {incremental_queue.queue_name} completed",
        priority=NotificationPriority.HIGH,
        notification_type=NotificationType.SUCCESS,
        data={
            "queue_name": incremental_queue.queue_name,
            "queue_id": incremental_queue.id,
            "chain_id": chain.id
        }
    )

