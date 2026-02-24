import traceback
from rest_framework.views import APIView
from keycloakauth.utils import IsAuthenticated
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Table, Status, ClientDataDump
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from nd_api.schemas.table_config import TableDetailsForUI


@conditional_authentication
class ReUseTableConfigView(APIView):
    authentication_classes = [IsAuthenticated]

    def _get_columns_from_config(self, table_config: TableDetailsForUI):
        all_cols = [col['column_name'] for col in table_config['columns_details']]
        return all_cols

    def get(self, request, client_id: int, dump_id: int):
        try:
            dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            total_dump_count = ClientDataDump.objects.filter(client_id=client_id).count()
            if total_dump_count < 2:
                return Response(
                    {
                        "message": f"previous dump not exists for dump-id: {dump_id}, client-id: {client_id}",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            remaining_tables = {"columns_mismatch": [], "no_table_in_prev_dump": []}
            for table in dump_obj.tables.all():
                any_prev_dump = (
                    ClientDataDump.objects.filter(
                        client_id=client_id,
                        dump_date__lt=dump_obj.dump_date,
                        tables__table_name=table.table_name  # ensures the dump contains this table
                    )
                    .order_by("-dump_date")
                    .first()
                )
                prev_table = any_prev_dump.tables.get(table_name=table.table_name) if any_prev_dump else None
                if prev_table:
                    current_cols = self._get_columns_from_config(table.table_details_for_ui)
                    prev_cols = self._get_columns_from_config(prev_table.table_details_for_ui)
                    if set(current_cols) == set(prev_cols):
                        table.table_details_for_ui = prev_table.table_details_for_ui
                        table.is_phi_marking_done = True
                        table.save()
                    else:
                        remaining_tables["columns_mismatch"].append((table.table_name, table.id))
                else:
                    remaining_tables["no_table_in_prev_dump"].append((table.table_name, table.id))
            return Response(
                {
                    "message": f"reused the table config for: dump-id: {dump_id}, client-id: {client_id}",
                    "remaining_tables": remaining_tables,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump not exists with dump-id: {dump_id}, client-id: {client_id}",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, client-id: {client_id}, dump-id: {dump_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
