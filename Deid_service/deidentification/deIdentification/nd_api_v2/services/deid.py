from django.conf import settings
from django.db import transaction
from typing import Optional
from nd_api_v2.models.ignore import IgnoreRowsDeIdentificaiton
from nd_api_v2.models.table_details import Table, Status, TableMetadata
from worker.models import Chain, Task
from core.dbPkg.dbhandler import NDDBHandler
from deIdentification.nd_logger import nd_logger
from core.process_df.main import start_de_identification_for_table_with_df
from nd_api.hooks import de_identification_failure_hook_for_table
from nd_api.views.utils import run_auto_qc, take_dump_and_upload_to_cloud
from ndwebsocket.utils import broadcast_task_status, broadcast_task_error
from ndwebsocket.models import NotificationPriority, NotificationType
from nd_api_v2.models.scheduler_config import SchedulerConfig
from nd_api_v2.models.configs import is_auto_qc_enabled, is_auto_gcp_enabled, is_auto_embd_enabled
from core.ops_df.jointables import CreateUpdateBridgeTable



def create_deidentification_task(table_obj: Table, chain: Optional[Chain] = None, delete_table: bool = True):
    chain_created_here = False
    if chain is None:
        chain, created = Chain.all_objects.get_or_create(
            reference_uuid=table_obj.get_deid_chain_reference_uuid()
        )
        chain_created_here = True
        if not created:
            chain.revive_and_save()
    tables_config = table_obj.metadata.table_details_for_ui
    
    batch_size = settings.BATCH_SIZE_DURING_DE_IDENTIFICATION
    table_obj.mark_as_in_progress_if_required()
    tasks = []
    all_tasks = []
    scheduler_config = SchedulerConfig.objects.last()
    if scheduler_config is None:
        raise Exception("Scheduler configuration not found")
    dest_connection: NDDBHandler = NDDBHandler(scheduler_config.get_deid_connection_str())
    
    if delete_table:
        dest_connection.drop_rows_from_table(table_obj.metadata.table_name, table_obj.nd_auto_increment_start_value, table_obj.nd_auto_increment_end_value)
        ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(queue_id=table_obj.incremental_queue.id, table_name=table_obj.metadata.table_name)
        nd_logger.info(f"Dropping ignore rows for {table_obj.metadata.table_name}, {table_obj.incremental_queue.id}")
        ignore_rows.delete()
    
    build_bridge_table_task = Task.create_task(
        fn=create_or_update_bridge_table,
        chain=chain,
        dependencies=[],
        arguments={"table_id": table_obj.id},
    )
    all_tasks.append(build_bridge_table_task)
    # adding max(rows_count, 1) to make sure atleast one task is created
    for offset in range(table_obj.nd_auto_increment_start_value, table_obj.nd_auto_increment_end_value+2, batch_size):
        task = Task.create_task(
            fn=start_de_identification_for_table_with_df,
            chain=chain,
            arguments={
                "table_id": table_obj.id,
                "batch_size": batch_size,
                "offset": {"gt": offset, "lt": offset + batch_size},
                "table_config": tables_config,
            },
            dependencies=[build_bridge_table_task],
            hooks={"failure": de_identification_failure_hook_for_table},
        )
        tasks.append(task)
        all_tasks.append(task)
    deid_final_task = Task.create_task(
            fn=mark_deid_completed,
            chain=chain,
            arguments={
                "table_id": table_obj.id,
            },
            dependencies=all_tasks,
            hooks={"failure": de_identification_failure_hook_for_table},
        )
    if is_auto_qc_enabled():
        auto_qc_task = Task.create_task(
            fn=run_auto_qc_task,
            chain=chain,
            dependencies=[deid_final_task],
            arguments={"table_id": table_obj.id, "chain_id": chain.id},
        )
        tasks.append(auto_qc_task)
        all_tasks.append(auto_qc_task)

        if is_auto_gcp_enabled():
            gcp_task = Task.create_task(
                fn=upload_to_gcp_task,
                chain=chain,
                dependencies=tasks,
                arguments={"table_id": table_obj.id, "chain_id": chain.id},
            )
            all_tasks.append(gcp_task)

        if is_auto_embd_enabled():
            embd_task = Task.create_task(
                fn=run_embedding_generation_task,
                chain=chain,
                dependencies=tasks,
                arguments={"table_id": table_obj.id, "chain_id": chain.id},
            )
            all_tasks.append(embd_task)
    
    if chain_created_here:
        cleanup_task = Task.create_task(
            fn=marked_complete_and_clean_up_tasks,
            chain=chain,
            dependencies=all_tasks,
            arguments={"table_id": table_obj.id, "chain_id": chain.id},
        )
        all_tasks.append(cleanup_task)
    return all_tasks, chain


def create_or_update_bridge_table(table_id: int, dependencies: list[Task] = []):
    table_obj = Table.objects.get(id=table_id)
    bridge = CreateUpdateBridgeTable(table_obj.id)
    bridge.build_bridge_table()
    return {"success": True}


def mark_deid_completed(table_id: int,  dependencies: list[Task] = []):
    table_obj = Table.objects.get(id=table_id)
    table_obj.deid.deid_status = Status.COMPLETED
    table_obj.deid.save()

    # Send structured table status update for UI
    from ndwebsocket.utils import broadcast_table_status_update
    broadcast_table_status_update(
        table_id=table_id,
        table_name=table_obj.metadata.table_name,
        process_type='deid',
        status='completed',
        message=f"De-identification completed for table {table_obj.metadata.table_name}",
        save_to_db=False
    )

    broadcast_task_status(
        status="completed",
        task_name=f"De-identification: {table_obj.metadata.table_name}",
        message=f"De-identification completed successfully for table {table_obj.metadata.table_name}",
        priority=NotificationPriority.HIGH,
        notification_type=NotificationType.SUCCESS,
        data={
            "table_name": table_obj.metadata.table_name,
            "deid_status": table_obj.deid.deid_status,
            "table_id": table_id
        }
    )
    
def run_auto_qc_task(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_obj = Table.objects.get(id=table_id)
    
    # Broadcast QC start
    broadcast_task_status(
        status="started",
        task_name=f"Auto QC: {table_obj.metadata.table_name}",
        message=f"Starting auto QC for table {table_obj.metadata.table_name}",
        priority=NotificationPriority.MEDIUM,
        notification_type=NotificationType.TASK_STATUS
    )
    
    try:
        run_auto_qc(table_id)
    except Exception as e:
        # Broadcast QC error
        broadcast_task_error(
            task_name=f"Auto QC: {table_obj.metadata.table_name}",
            error=f"Auto QC failed for table {table_obj.metadata.table_name}: {str(e)}",
            error_code="QC_ERROR",
            details={"table_id": table_id, "chain_id": chain_id}
        )
        raise


def upload_to_gcp_task(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_obj = Table.objects.get(id=table_id)
    
    # Send GCP upload start status
    from ndwebsocket.utils import broadcast_table_status_update
    broadcast_table_status_update(
        table_id=table_id,
        table_name=table_obj.metadata.table_name,
        process_type='gcp',
        status='in_progress',
        message=f"GCP upload started for table {table_obj.metadata.table_name}",
        save_to_db=False
    )
    
    try:
        take_dump_and_upload_to_cloud(table_id, reupload=True)
        
        # Send GCP upload completion status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.metadata.table_name,
            process_type='gcp',
            status='completed',
            message=f"GCP upload completed for table {table_obj.metadata.table_name}",
            save_to_db=False
        )
        
        # Broadcast GCP upload completion (keeping for backward compatibility)
        broadcast_task_status(
            status="completed",
            task_name=f"GCP Upload: {table_obj.metadata.table_name}",
            message=f"GCP upload completed successfully for table {table_obj.metadata.table_name}",
            priority=NotificationPriority.MEDIUM,
            notification_type=NotificationType.SUCCESS
        )
    except Exception as e:
        # Send GCP upload failure status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.metadata.table_name,
            process_type='gcp',
            status='failed',
            message=f"GCP upload failed for table {table_obj.metadata.table_name}",
            error_details={"error": str(e), "chain_id": chain_id},
            save_to_db=False
        )
        
        # Broadcast GCP upload error (keeping for backward compatibility)
        broadcast_task_error(
            task_name=f"GCP Upload: {table_obj.metadata.table_name}",
            error=f"GCP upload failed for table {table_obj.metadata.table_name}: {str(e)}",
            error_code="GCP_UPLOAD_ERROR",
            details={"table_id": table_id, "chain_id": chain_id}
        )
        raise


def marked_complete_and_clean_up_tasks(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):

    chain = Chain.objects.get(id=chain_id)
    with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
        chain.soft_delete_and_save()


def run_embedding_generation_task(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_obj = Table.objects.get(id=table_id)
    
    # Send embedding generation start status
    from ndwebsocket.utils import broadcast_table_status_update
    broadcast_table_status_update(
        table_id=table_id,
        table_name=table_obj.metadata.table_name,
        process_type='embd',
        status='in_progress',
        message=f"Embedding generation started for table {table_obj.metadata.table_name}",
        save_to_db=False
    )
    
    try:
        table_obj.embd.embd_stats = Status.COMPLETED
        table_obj.embd.save()
        
        # Send embedding generation completion status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.metadata.table_name,
            process_type='embd',
            status='completed',
            message=f"Embedding generation completed for table {table_obj.metadata.table_name}",
            save_to_db=False
        )
        
        # Broadcast embedding generation completion (keeping for backward compatibility)
        broadcast_task_status(
            status="completed",
            task_name=f"Embedding Generation: {table_obj.metadata.table_name}",
            message=f"Embedding generation completed successfully for table {table_obj.metadata.table_name}",
            priority=NotificationPriority.MEDIUM,
            notification_type=NotificationType.SUCCESS
        )
    except Exception as e:
        # Send embedding generation failure status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.metadata.table_name,
            process_type='embd',
            status='failed',
            message=f"Embedding generation failed for table {table_obj.metadata.table_name}",
            error_details={"error": str(e), "chain_id": chain_id},
            save_to_db=False
        )
        
        # Broadcast embedding generation error (keeping for backward compatibility)
        broadcast_task_error(
            task_name=f"Embedding Generation: {table_obj.metadata.table_name}",
            error=f"Embedding generation failed for table {table_obj.metadata.table_name}: {str(e)}",
            error_code="EMBEDDING_ERROR",
            details={"table_id": table_id, "chain_id": chain_id}
        )
        raise

# def run_indexing_task(
#     table_id: int, chain_id: int, dependencies: list[Task] = []
# ):
#     table_obj = Table.objects.get(id=table_id)