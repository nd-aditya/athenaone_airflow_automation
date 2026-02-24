import traceback
from django.conf import settings
from django.db import transaction
from rest_framework import status
from worker.models import Task, Chain
from rest_framework.views import APIView
from core.dbPkg.dbhandler import NDDBHandler
from rest_framework.response import Response
from nd_api.models import Table, ClientDataDump, IgnoreRowsDeIdentificaiton, Status
from core.process.main import start_de_identification_for_table
from core.process_df.main import start_de_identification_for_table_with_df
from nd_api.hooks import de_identification_failure_hook_for_table
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from tqdm import tqdm
from .utils import run_auto_qc, take_dump_and_upload_to_cloud
from ndwebsocket.utils import (
    broadcast_task_status, 
    broadcast_task_progress, 
    broadcast_task_error,
    save_notification_to_db
)
from ndwebsocket.models import NotificationType, NotificationPriority


@conditional_authentication
class StopDeIdentificationView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, table_id: int):
        try:
            try:
                table_obj = Table.objects.get(id=table_id)
            except Table.DoesNotExist:
                # Broadcast error notification
                broadcast_task_error(
                    task_name=f"Stop De-identification",
                    error=f"Table with ID {table_id} not found",
                    error_code="TABLE_NOT_FOUND"
                )
                return Response({"message": "Table details not found", "success": False}, status=status.HTTP_400_BAD_REQUEST)
            with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
                try:
                    chain = Chain.all_objects.get(
                        reference_uuid=table_obj.get_deid_chain_reference_uuid()
                    )
                    chain.soft_delete_and_save()
                except Chain.DoesNotExist as e:
                    pass
                table_obj.marked_as_not_started()
            
            # Broadcast successful stop
            broadcast_task_status(
                status="completed",
                task_name=f"Stop De-identification: {table_obj.table_name}",
                message=f"De-identification stopped successfully for table {table_obj.table_name}",
                priority=NotificationPriority.MEDIUM,
                notification_type=NotificationType.SUCCESS
            )
            
            nd_logger.info(f"De Identification stopped successfully, table_id: {table_id}")
            return Response({"message": "De Identification stopped successfully", "success": False}, status.HTTP_200_OK)
        except Exception as e:
            message = f"StopDeIdentificationView.post: Internal server error : {e}, for user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            
            # Broadcast error notification
            broadcast_task_error(
                task_name=f"Stop De-identification",
                error=f"Failed to stop de-identification for table ID {table_id}: {str(e)}",
                error_code="STOP_DEID_ERROR",
                details={"table_id": table_id, "user": str(request.user)}
            )
            
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        
        
@conditional_authentication
class DeIdentifyTableView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, table_id: int):
        try:
            try:
                table_obj = Table.objects.get(id=table_id)
            except Table.DoesNotExist:
                # Broadcast error notification
                broadcast_task_error(
                    task_name=f"Start De-identification",
                    error=f"Table with ID {table_id} does not exist",
                    error_code="TABLE_NOT_FOUND"
                )
                return Response(
                    {"message": "table_id does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            
            if not table_obj.is_phi_marking_locked:
                return Response(
                    {
                        "message": "Failed, cannot start deIdentifiation, PHI marking is not locked",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            
            # Broadcast start of de-identification
            broadcast_task_status(
                status="started",
                task_name=f"De-identification: {table_obj.table_name}",
                message=f"Starting de-identification for table {table_obj.table_name}",
                priority=NotificationPriority.HIGH,
                notification_type=NotificationType.TASK_STATUS
            )
            
            tasks, chain = create_deidentification_task(table_obj=table_obj)
            message = f"DeIdentifyTableView.get: Table de-identification started successfully, table_id: {table_id}, {request.user}"
            nd_logger.error(message)
            return Response(
                {
                    "message": "Table de-identification started successfully",
                    "success": True,
                    "chain_id": chain.id,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"DeIdentifyTableView.get: Internal server error : {e}, for user: {request.user}, table_id: {table_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            
            # Broadcast error notification
            broadcast_task_error(
                task_name=f"Start De-identification",
                error=f"Failed to start de-identification for table ID {table_id}: {str(e)}",
                error_code="START_DEID_ERROR",
                details={"table_id": table_id, "user": str(request.user)}
            )
            
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class TableDeidBulkView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, client_id: int, dump_id: int):
        data = request.data
        tables_obj = []
        try:
            dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            tables_ids = data.get('tables_id', [])
            if len(tables_ids)<1:
                tables_name = data.get('tables_name', [])
                if len(tables_name) < 1:
                    return Response(
                        {"message": "No table IDs/tables-names provided", "success": False},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                tables_obj = Table.objects.filter(table_name__in=tables_name, dump=dump_obj)
            else:
                tables_obj = Table.objects.filter(id__in=tables_ids, dump=dump_obj)
        except ClientDataDump.DoesNotExist:
            return Response(
                {"message": f"dump not exists with dump-id: {dump_id}, client-id: {client_id}", "success": False},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=dump_obj.get_chain_reference_uuid_for_bulk_deid()
            )
            task = Task.create_task(
                fn=create_deid_tasks_for_tables,
                chain=chain,
                arguments={
                    "dump_id": dump_id,
                    "tables_names": list(tables_obj.values_list('table_name', flat=True))
                }
            )
            return Response(
                {
                    "message": f"De Identification for the Tables: {tables_ids} started successfully",
                    "success": True,
                    "successfull_tables": []
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, data-provided: {data}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

def create_deid_tasks_for_tables(dump_id: int, tables_names: list[str], dependencies: list[Task] = []):
    tables_obj = Table.objects.filter(table_name__in=tables_names, dump=dump_id).order_by('priority')
    pushed_tables = []
    for table_obj in tqdm(tables_obj, "Creating de-identification tasks for tables"):
        create_deidentification_task(table_obj, use_df=True)
        pushed_tables.append(table_obj.table_name)
    return pushed_tables

def create_deidentification_task(table_obj: Table, delete_table: bool = True, use_df: bool = True):
    chain, created = Chain.all_objects.get_or_create(
        reference_uuid=table_obj.get_deid_chain_reference_uuid()
    )
    if not created:
        chain.revive_and_save()
    tables_config = table_obj.table_details_for_ui
    
    batch_size = settings.BATCH_SIZE_DURING_DE_IDENTIFICATION
    table_obj.mark_as_in_progress_if_required()
    tasks = []
    all_tasks = []
    dest_connection: NDDBHandler = table_obj.dump.get_destination_db_connection()
    
    if delete_table:
        dest_connection.drop_table(table_obj.table_name)
        ignore_rows = IgnoreRowsDeIdentificaiton.objects.filter(dump_name=table_obj.dump.dump_name, table_name=table_obj.table_name)
        nd_logger.info(f"Dropping ignore rows for {table_obj.table_name}, {table_obj.dump.dump_name}")
        ignore_rows.delete()
    
    # adding max(rows_count, 1) to make sure atleast one task is created
    for offset in range(0, max(table_obj.rows_count, 1), batch_size):
        deid_fn = start_de_identification_for_table_with_df if use_df else start_de_identification_for_table
        task = Task.create_task(
            fn=deid_fn,
            chain=chain,
            arguments={
                "table_id": table_obj.id,
                "batch_size": batch_size,
                "offset": offset,
                "table_config": tables_config,
            },
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
            hooks={"failure": de_identification_failure_hook_for_table},
        )
    is_auto_qc_enabled = table_obj.dump.is_auto_qc_enabled()
    if is_auto_qc_enabled:
        auto_qc_task = Task.create_task(
            fn=run_auto_qc_task,
            chain=chain,
            dependencies=[deid_final_task],
            arguments={"table_id": table_obj.id, "chain_id": chain.id},
        )
        tasks.append(auto_qc_task)
        all_tasks.append(auto_qc_task)

        is_auto_gcp_upload_enabled = table_obj.dump.is_auto_gcp_upload_enabled()
        if is_auto_gcp_upload_enabled:
            gcp_task = Task.create_task(
                fn=upload_to_gcp_task,
                chain=chain,
                dependencies=tasks,
                arguments={"table_id": table_obj.id, "chain_id": chain.id},
            )
            all_tasks.append(gcp_task)

        is_auto_embd_enabled = table_obj.dump.is_auto_embd_enabled()
        if is_auto_embd_enabled:
            embd_task = Task.create_task(
                fn=run_embedding_generation_task,
                chain=chain,
                dependencies=tasks,
                arguments={"table_id": table_obj.id, "chain_id": chain.id},
            )
            all_tasks.append(embd_task)
        
    cleanup_task = Task.create_task(
        fn=marked_complete_and_clean_up_tasks,
        chain=chain,
        dependencies=all_tasks,
        arguments={"table_id": table_obj.id, "chain_id": chain.id},
    )
    all_tasks.append(cleanup_task)
    return all_tasks, chain

def mark_deid_completed(table_id: int,  dependencies: list[Task] = []):
    table_obj = Table.objects.get(id=table_id)
    table_obj.deid.deid_status = Status.COMPLETED
    table_obj.deid.save()

    # Send structured table status update for UI
    from ndwebsocket.utils import broadcast_table_status_update
    broadcast_table_status_update(
        table_id=table_id,
        table_name=table_obj.table_name,
        process_type='deid',
        status='completed',
        message=f"De-identification completed for table {table_obj.table_name}",
        save_to_db=False
    )

    # Broadcast successful completion (keeping for backward compatibility)
    broadcast_task_status(
        status="completed",
        task_name=f"De-identification: {table_obj.table_name}",
        message=f"De-identification completed successfully for table {table_obj.table_name}",
        priority=NotificationPriority.HIGH,
        notification_type=NotificationType.SUCCESS,
        data={
            "table_name": table_obj.table_name,
            "deid_status": table_obj.deid.deid_status,
            "table_id": table_id
        }
    )
    

def marked_complete_and_clean_up_tasks(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    chain = Chain.objects.get(id=chain_id)
    with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
        chain.soft_delete_and_save()

def run_auto_qc_task(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_obj = Table.objects.get(id=table_id)
    
    # Broadcast QC start
    broadcast_task_status(
        status="started",
        task_name=f"Auto QC: {table_obj.table_name}",
        message=f"Starting auto QC for table {table_obj.table_name}",
        priority=NotificationPriority.MEDIUM,
        notification_type=NotificationType.TASK_STATUS
    )
    
    try:
        run_auto_qc(table_id)
    except Exception as e:
        # Broadcast QC error
        broadcast_task_error(
            task_name=f"Auto QC: {table_obj.table_name}",
            error=f"Auto QC failed for table {table_obj.table_name}: {str(e)}",
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
        table_name=table_obj.table_name,
        process_type='gcp',
        status='in_progress',
        message=f"GCP upload started for table {table_obj.table_name}",
        save_to_db=False
    )
    
    try:
        take_dump_and_upload_to_cloud(table_id, reupload=True)
        
        # Send GCP upload completion status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.table_name,
            process_type='gcp',
            status='completed',
            message=f"GCP upload completed for table {table_obj.table_name}",
            save_to_db=False
        )
        
        # Broadcast GCP upload completion (keeping for backward compatibility)
        broadcast_task_status(
            status="completed",
            task_name=f"GCP Upload: {table_obj.table_name}",
            message=f"GCP upload completed successfully for table {table_obj.table_name}",
            priority=NotificationPriority.MEDIUM,
            notification_type=NotificationType.SUCCESS
        )
    except Exception as e:
        # Send GCP upload failure status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.table_name,
            process_type='gcp',
            status='failed',
            message=f"GCP upload failed for table {table_obj.table_name}",
            error_details={"error": str(e), "chain_id": chain_id},
            save_to_db=False
        )
        
        # Broadcast GCP upload error (keeping for backward compatibility)
        broadcast_task_error(
            task_name=f"GCP Upload: {table_obj.table_name}",
            error=f"GCP upload failed for table {table_obj.table_name}: {str(e)}",
            error_code="GCP_UPLOAD_ERROR",
            details={"table_id": table_id, "chain_id": chain_id}
        )
        raise

def run_embedding_generation_task(
    table_id: int, chain_id: int, dependencies: list[Task] = []
):
    table_obj = Table.objects.get(id=table_id)
    
    # Send embedding generation start status
    from ndwebsocket.utils import broadcast_table_status_update
    broadcast_table_status_update(
        table_id=table_id,
        table_name=table_obj.table_name,
        process_type='embd',
        status='in_progress',
        message=f"Embedding generation started for table {table_obj.table_name}",
        save_to_db=False
    )
    
    try:
        table_obj.embd.embd_stats = Status.COMPLETED
        table_obj.embd.save()
        
        # Send embedding generation completion status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.table_name,
            process_type='embd',
            status='completed',
            message=f"Embedding generation completed for table {table_obj.table_name}",
            save_to_db=False
        )
        
        # Broadcast embedding generation completion (keeping for backward compatibility)
        broadcast_task_status(
            status="completed",
            task_name=f"Embedding Generation: {table_obj.table_name}",
            message=f"Embedding generation completed successfully for table {table_obj.table_name}",
            priority=NotificationPriority.MEDIUM,
            notification_type=NotificationType.SUCCESS
        )
    except Exception as e:
        # Send embedding generation failure status
        broadcast_table_status_update(
            table_id=table_id,
            table_name=table_obj.table_name,
            process_type='embd',
            status='failed',
            message=f"Embedding generation failed for table {table_obj.table_name}",
            error_details={"error": str(e), "chain_id": chain_id},
            save_to_db=False
        )
        
        # Broadcast embedding generation error (keeping for backward compatibility)
        broadcast_task_error(
            task_name=f"Embedding Generation: {table_obj.table_name}",
            error=f"Embedding generation failed for table {table_obj.table_name}: {str(e)}",
            error_code="EMBEDDING_ERROR",
            details={"table_id": table_id, "chain_id": chain_id}
        )
        raise

# def run_indexing_task(
#     table_id: int, chain_id: int, dependencies: list[Task] = []
# ):
#     table_obj = Table.objects.get(id=table_id)