from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Table, Status
from keycloakauth.utils import IsAuthenticated
from worker.models import Task, Chain
from nd_api.decorator import conditional_authentication
from nd_api.models import ClientDataDump
from deIdentification.nd_logger import nd_logger
import traceback
from tqdm import tqdm


@conditional_authentication
class InterruptDeidView(APIView):
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
                reference_uuid=dump_obj.get_chain_reference_uuid_for_bulk_deid_interrupt()
            )
            task = Task.create_task(
                fn=interrupt_deid_for_tables,
                chain=chain,
                arguments={
                    "dump_id": dump_id,
                    "tables_ids": list(tables_obj.values_list('id', flat=True))
                }
            )
            return Response(
                {
                    "message": f"De Identification for the Tables: {tables_ids} interrupted successfully",
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

def interrupt_deid_for_tables(dump_id: int, tables_ids: list[int], dependencies: list[Task] = []):
    tables_obj: list[Table] = Table.objects.filter(id__in=tables_ids, dump=dump_id)

    for table_obj in tables_obj:
        if table_obj.deid.deid_status != Status.IN_PROGRESS:
            continue
        chain = Chain.all_objects.filter(
            reference_uuid=table_obj.get_deid_chain_reference_uuid()
        ).first()
        if chain is None:
            continue
        chain.interrupt()
        table_obj.deid.deid_status = Status.INTERUPTED
        table_obj.deid.failure_remarks = {"remarks": "Interrupted by user"}
        table_obj.deid.save()



@conditional_authentication
class InterruptQCView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, client_id: int, dump_id : int):
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
                reference_uuid=dump_obj.get_chain_reference_uuid_for_bulk_qc_interrupt()
            )
            task = Task.create_task(
                fn=interrupt_qc_for_tables,
                chain=chain,
                arguments={
                    "dump_id": dump_id,
                    "tables_ids": list(tables_obj.values_list('id', flat=True))
                }
            )
            return Response(
                {
                    "message": f"QC for the Tables: {tables_ids} interrupted successfully",
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

def interrupt_qc_for_tables(dump_id: int, tables_ids: list[int], dependencies: list[Task] = []):
    try:
        tables_obj: list[Table] = Table.objects.filter(id__in=tables_ids, dump=dump_id)

        for table_obj in tqdm(tables_obj, desc="Interrupting QC for tables", total=len(tables_obj)):
            if table_obj.qc.qc_status != Status.IN_PROGRESS:
                continue
            chain = Chain.all_objects.filter(
                reference_uuid=table_obj.get_qc_chain_reference_uuid()
            ).first()
            if chain is None:
                continue
            chain.interrupt()
            table_obj.qc.qc_status = Status.INTERUPTED
            table_obj.qc.failure_remarks = {"remarks": "Interrupted by user"}
            table_obj.qc.save()
    except Exception as e:
        message = f"Internal server error: {e}"
        nd_logger.error(message)
        nd_logger.error(traceback.format_exc())
        raise Exception(message)
