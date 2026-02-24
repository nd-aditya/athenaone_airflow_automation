import traceback
from rest_framework.views import APIView
from keycloakauth.utils import IsAuthenticated
from django.conf import settings
from rest_framework import status
from worker.models import Task, Chain
from rest_framework.response import Response
from nd_api.models import Table, Status, ClientDataDump
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from ndwebsocket.utils import broadcast_task_status, broadcast_task_error
from ndwebsocket.models import NotificationType, NotificationPriority
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from nd_api.views.utils import register_table_and_generate_analytics
from portal.alerts import alert_sender


@conditional_authentication
class RefershSourceDbView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, dump_id: int):
        try:
            dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=f"refresh_stats_generation_{dump_obj.id}"
            )
            task = Task.create_task(
                chain=chain,
                fn=refresh_stats_generation_task,
                arguments={"dump_id": dump_obj.id},
                dependencies=[],
            )
            return Response(
                {"message": f"successfully created task for refresh db", "success": True},
                status=status.HTTP_200_OK,
            )
        except ClientDataDump.DoesNotExist:
            return Response(
                {"message": f"dump not exists with dump-id: {dump_id}, client-id: {client_id}", "success": False},
                status=status.HTTP_400_BAD_REQUEST,
            )
        
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, dump_id: {dump_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def refresh_stats_generation_task(
    dump_id: int, dependencies: list[Task] = []
):
    try:
        nd_logger.info(f"inside refresh_stats_generation_task for dump-id:  {dump_id}")
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        
        # Broadcast stats generation start
        broadcast_task_status(
            status="started",
            task_name=f"Refresh Stats Generation: {dump_obj.dump_name}",
            message=f"Starting stats generation for dump {dump_obj.dump_name}",
            priority=NotificationPriority.HIGH,
            notification_type=NotificationType.TASK_STATUS,
            data={"dump_id": dump_id, "client_id": dump_obj.client.id}
        )
        
        source_db_connection = dump_obj.get_source_db_connection()
        all_tables = source_db_connection.get_all_tables()
        dump_stats = {"tables_stats": {}}

        # deleting the tables which are not present in the source database
        tables_deleted = Table.objects.exclude(table_name__in=all_tables).filter(dump=dump_obj)
        nd_logger.info(f"Deleting the tables which are deleted from dump: {dump_obj.id}, counts: {tables_deleted.count()}")
        tables_deleted.delete()
        
        new_tables = []
        for table_name in all_tables:
            try:
                Table.objects.get(table_name=table_name, dump=dump_obj)
            except Table.DoesNotExist:
                new_tables.append(table_name)
        

        # Adjust max_workers based on your system capabilities
        rerun = False
        with ThreadPoolExecutor(
            max_workers=settings.STATS_GENERATION_MAX_WORKER_COUNT
        ) as executor:
            future_to_table = {
                executor.submit(register_table_and_generate_analytics, table, dump_obj, rerun): table
                for table in new_tables
            }

            for future in tqdm(
                as_completed(future_to_table),
                total=len(all_tables),
                desc="Generating table stats",
            ):
                table, table_stats, rows_count = future.result()
                dump_stats["tables_stats"][table] = table_stats

        dump_obj.dump_stats = dump_stats
        dump_obj.save()
        dump_obj.marked_stats_generation_as_completed()
        
        # Broadcast successful completion
        broadcast_task_status(
            status="completed",
            task_name=f"Refresh Source Db: {dump_obj.dump_name}",
            message=f"Refresh Source Db task completed successfully for {len(all_tables)} tables",
            priority=NotificationPriority.HIGH,
            notification_type=NotificationType.SUCCESS,
            data={
                "dump_id": dump_id,
                "client_id": dump_obj.client.id,
                "tables_processed": len(all_tables)
            }
        )
        
        return {}
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"Refresh Source Db task: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            error=f"Stats generation failed for dump ID {dump_id}: {str(e)}",
            error_code="STATS_GENERATION_ERROR",
            details={
                "dump_id": dump_id,
                "client_id": dump_obj.client.id if 'dump_obj' in locals() else None,
                "rerun": False
            }
        )
        
        prepare_message = {
            "alert_type": "Refresh Source Db task Failed",
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
            "client_identifier": f"{dump_obj.client.id if 'dump_obj' in locals() else 'Unknown'} - {dump_obj.client.client_name if 'dump_obj' in locals() else 'Unknown'}",
            "traceback": traceback.format_exc()
        }
        alert_sender.send_message(prepare_message)
        raise e
