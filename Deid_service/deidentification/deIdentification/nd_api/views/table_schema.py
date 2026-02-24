
import traceback
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Clients, Table, ClientDataDump
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class TablesSchemaView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, dump_id: int):
        try:
            tables_ids = request.query_params.get("tables_ids", [])
            tables_ids = []
            try:
                dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
                if len(tables_ids)>0:
                    tables = Table.objects.filter(id__in=tables_ids, dump=dump_obj)
                else:
                    tables = Table.objects.filter(dump=dump_obj)
            except ClientDataDump.DoesNotExist:
                return Response(
                    {"message": f"dump-id: {dump_id}, does not exists"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            tables_info = []
            for table in tables:
                columns = [_d['column_name'] for _d in table.table_details_for_ui['columns_details']]
                tables_info.append({'table_id': table.id, 'table_name': table.table_name, 'columns': columns})
            response_json = {
                "patient_identifier_columns": dump_obj.client.patient_identifier_columns,
                "tables_info": tables_info
            }
            return Response(response_json, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
