import traceback
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from nd_api_v2.decorator import conditional_authentication
from worker.models import Chain, Task
from deIdentification.nd_logger import nd_logger
from keycloakauth.utils import IsAuthenticated
from nd_api_v2.decorator import conditional_authentication
from nd_api_v2.models import IncrementalQueue,Table
from tqdm import tqdm
from nd_api_v2.services.deid import create_deidentification_task


@conditional_authentication
class StartDeIdentificationView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, queue_id: int):
        data = request.data
        tables_obj = []
        try:
            queue_obj: IncrementalQueue = IncrementalQueue.objects.get(id=queue_id)
            table_ids = data.get('table_ids', [])
            if len(table_ids)<1:
                tables_name = data.get('tables_name', [])
                if len(tables_name) < 1:
                    return Response(
                        {"message": "No table IDs/tables-names provided", "success": False},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                tables_obj = Table.objects.filter(metadata__table_name__in=tables_name, incremental_queue=queue_obj)
            else:
                tables_obj = Table.objects.filter(id__in=table_ids, incremental_queue=queue_obj)
        except IncrementalQueue.DoesNotExist:
            return Response(
                {"message": f"queue not exists with queue-id: {queue_id}", "success": False},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=queue_obj.get_deid_chain_reference_uuid()
            )
            task = Task.create_task(
                fn=create_deid_tasks_for_tables,
                chain=chain,
                arguments={
                    "queue_id": queue_id,
                    "tables_ids": list(tables_obj.values_list('id', flat=True))
                }
            )
            return Response(
                {
                    "message": f"De Identification for the Tables: {table_ids} started successfully",
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

def create_deid_tasks_for_tables(queue_id: int, tables_ids: list[int], dependencies: list[Task] = []):
    tables_obj = Table.objects.filter(id__in=tables_ids, incremental_queue=queue_id).order_by('metadata__priority')
    pushed_tables = []
    for table_obj in tqdm(tables_obj, "Creating de-identification tasks for tables"):
        create_deidentification_task(table_obj)
        pushed_tables.append(table_obj.id)
    return pushed_tables
