import traceback
from rest_framework.views import APIView
from keycloakauth.utils import IsAuthenticated
from django.conf import settings
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Table, Status, ClientDataDump
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class TableEmbeddingView(APIView):
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
            for table_obj in tables_obj:
                table_obj.embd.embd_stats = Status.IN_PROGRESS
                table_obj.embd.save()
                
                # Send websocket update for embedding started
                from ndwebsocket.utils import broadcast_table_status_update
                broadcast_table_status_update(
                    table_id=table_obj.id,
                    table_name=table_obj.table_name,
                    process_type='embd',
                    status='in_progress',
                    message=f"Embedding generation started for table {table_obj.table_name}",
                    save_to_db=False
                )
            return Response(
                {
                    "message": f"Embedding generation for the Tables started successfully",
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, data: {data}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

@conditional_authentication
class EmbeddingResultView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, dump_id: int):
        dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
        try:
            tables_obj = Table.objects.filter(dump=dump_obj)
            response_json = []
            for table_obj in tables_obj:
                response_json.append({
                    'table_id': table_obj.id,
                    'embd_result': table_obj.embd.failure_remarks,
                    'embd_stats': table_obj.embd.embd_stats
                })
            return Response(response_json, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

