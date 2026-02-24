from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from nd_api_v2.decorator import conditional_authentication
from nd_api_v2.models.table_details import Table, Status
from worker.models import Chain, Task
from deIdentification.nd_logger import nd_logger
from keycloakauth.utils import IsAuthenticated
from nd_api_v2.decorator import conditional_authentication
from nd_api_v2.models import IncrementalQueue
import traceback
from django.conf import settings
from qc_package.scanner import DbScanner
from tqdm import tqdm


@conditional_authentication
class QCBulkView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, queue_id: int):
        data = request.data
        tables_obj = []
        try:
            queue_obj = IncrementalQueue.objects.get(id=queue_id)
            tables_ids = data.get('tables_id', [])
            if len(tables_ids)<1:
                tables_name = data.get('tables_name', [])
                if len(tables_name) < 1:
                    return Response(
                        {"message": "No table IDs/tables-names provided", "success": False},
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                tables_obj = Table.objects.filter(metadata__table_name__in=tables_name, incremental_queue=queue_obj)
            else:
                tables_obj = Table.objects.filter(id__in=tables_ids, incremental_queue=queue_obj)
        except IncrementalQueue.DoesNotExist:
            return Response(
                {"message": f"queue not exists with queue-id: {queue_id}", "success": False},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=queue_obj.get_chain_reference_uuid_for_bulk_qc()
            )
            task = Task.create_task(
                fn=run_qc_for_tables,
                chain=chain,
                arguments={
                    "queue_id": queue_id,
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


def run_qc_for_tables(queue_id: int, tables_ids: list[int], dependencies: list[Task] = []):
    tables_obj: list[Table] = Table.objects.filter(id__in=tables_ids, incremental_queue__id=queue_id)

    for table_obj in tqdm(tables_obj, desc="Running QC for tables", total=len(tables_obj)):
        if table_obj.qc.qc_status == Status.IN_PROGRESS:
            continue
        run_auto_qc(table_id=table_obj.id, read_limit=settings.QC_READ_LIMIT)
        table_obj.qc.qc_status = Status.IN_PROGRESS
        table_obj.qc.save()

def run_auto_qc(table_id: int, read_limit: int = 10000):
    try:
        db_scanner = DbScanner()
        result = db_scanner.scan_table(table_id, read_limit)
    except Table.DoesNotExist:
        nd_logger.error(f"QC Task failed: Table ID {table_id} not found.")
        raise Exception(f"QC Task failed: Table ID {table_id} not found.")
    except Exception as e:
        nd_logger.error(f"QC Task error: {e}, table_id: {table_id}")
        nd_logger.error(traceback.format_exc())
        raise Exception(f"QC Task error: {e}, table_id: {table_id}")

