import traceback
from rest_framework.views import APIView
from keycloakauth.utils import IsAuthenticated
from django.conf import settings
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Table, Status, ClientDataDump
from worker.models import Task, Chain
from deIdentification.nd_logger import nd_logger
from qc_package.scanner import DbScanner
from nd_api.decorator import conditional_authentication
from ndwebsocket.utils import (
    broadcast_task_status, 
    broadcast_task_progress, 
    broadcast_task_error,
    save_notification_to_db
)
from ndwebsocket.models import NotificationType, NotificationPriority
from .utils import run_auto_qc


@conditional_authentication
class QCResultView(APIView):
    authentication_classes = [IsAuthenticated]
    
    def _safe_json_response(self, qc_result: dict):
        nd_auto_ids = qc_result['final_qc_result']['failure_nd_auto_incr_ids']
        str_nd_auto_ids = [str(_id) for _id in nd_auto_ids]
        qc_result['final_qc_result']['failure_nd_auto_incr_ids'] = str_nd_auto_ids
        return qc_result
    
    def get(self, request, client_id: int, dump_id: int):
        # TODO using get_tables from frontend and not this get call
        dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
        try:
            tables_obj = Table.objects.filter(dump=dump_obj)

            response_json = []
            completed_count = 0
            in_progress_count = 0
            failed_count = 0
            
            for table_obj in tables_obj:
                qc_status = table_obj.qc.qc_status
                if qc_status == Status.COMPLETED:
                    completed_count += 1
                elif qc_status == Status.IN_PROGRESS:
                    in_progress_count += 1
                elif qc_status == Status.FAILED:
                    failed_count += 1
                
                response_json.append({
                    'table_id': table_obj.id,
                    'qc_result': self._safe_json_response(table_obj.qc.qc_result),
                    'qc_stats': table_obj.qc.qc_status
                })
            return Response(response_json, status=status.HTTP_200_OK)
            
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            
            # Broadcast error notification
            broadcast_task_error(
                task_name=f"QC Results: {dump_obj.dump_name if 'dump_obj' in locals() else 'Unknown'}",
                error=f"Failed to retrieve QC results: {str(e)}",
                error_code="QC_RESULTS_ERROR",
                details={
                    "dump_id": dump_id, 
                    "client_id": client_id, 
                    "user": str(request.user)
                }
            )
            
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )



@conditional_authentication
class QCBulkView(APIView):
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
                reference_uuid=dump_obj.get_chain_reference_uuid_for_bulk_qc()
            )
            task = Task.create_task(
                fn=run_qc_for_tables,
                chain=chain,
                arguments={
                    "dump_id": dump_id,
                    "tables_ids": list(tables_obj.values_list('id', flat=True))
                }
            )
            return Response(
                {
                    "message": f"QC for the Tables: {tables_ids} started successfully",
                    "success": True,
                    "successfull_tables": []
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, data-provided: {data}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())


def run_qc_for_tables(dump_id: int, tables_ids: list[int], dependencies: list[Task] = []):
    tables_obj: list[Table] = Table.objects.filter(id__in=tables_ids, dump=dump_id)

    for table_obj in tables_obj:
        if table_obj.qc.qc_status == Status.IN_PROGRESS:
            continue
        run_auto_qc(table_id=table_obj.id, read_limit=settings.QC_READ_LIMIT)
        table_obj.qc.qc_status = Status.IN_PROGRESS
        table_obj.qc.save()