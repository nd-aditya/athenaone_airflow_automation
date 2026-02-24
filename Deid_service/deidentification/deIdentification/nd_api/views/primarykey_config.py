import traceback
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import ClientDataDump, Table
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
import pandas as pd
import json
from deIdentification.nd_logger import nd_logger


@conditional_authentication
class PrimaryKeyConfigView(APIView):
    authentication_classes = [IsAuthenticated]

    def _update_primary_key_for_table(
        self, dump_obj: ClientDataDump, tables_with_primary_keys: list[dict]
    ):
        for primary_key_mapping in tables_with_primary_keys:
            table_name, primary_key = primary_key_mapping
            try:
                table: Table = dump_obj.tables.get(table_name=table_name)
                table.metadata.primary_key = {"primary_key": primary_key.split("|")}
                table.metadata.save()
            except Table.DoesNotExist as e:
                nd_logger.error(f"table {table_name}, does not exists, ignoring for this table")

    def _read_table_with_primary_key_mappnig(self, request):
        csv_file = request.FILES.get("file")
        if not csv_file:
            return Response(
                {"error": "CSV file not provided."}, status=status.HTTP_400_BAD_REQUEST
            )
        df = pd.read_csv(csv_file)
        table_key_mapping = list(
            df[["table_name", "unique_key_column"]].itertuples(index=False, name=None)
        )
        return table_key_mapping

    def post(self, request, client_id: int, dump_id: int):
        try:
            client_dump = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            data = json.loads(request.data.get("data", "{}"))
            use_previously_uploaded_mapping = data.get("use_previously_uploaded_mapping", False)
            total_client_dumps = client_dump.client.dumps.count()
            if use_previously_uploaded_mapping and total_client_dumps < 2:
                return Response(
                    {
                        "message": f"use_previously_uploaded_mapping is : {use_previously_uploaded_mapping}, but its total-dump-count: {total_client_dumps}",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if use_previously_uploaded_mapping:
                update_primary_keys_using_previous_dumps(client_dump)
                tables_with_no_primary_keys = get_table_ids_with_no_primary_key_mapping(
                    client_dump
                )
                client_dump.is_primary_key_uploaded = True
                client_dump.save()
                return Response(
                    {
                        "message": f"use_previously_uploaded_mapping is : {use_previously_uploaded_mapping}",
                        "success": True,
                        "tables_with_no_primary_key": tables_with_no_primary_keys,
                    },
                    status=status.HTTP_200_OK,
                )
            else:
                table_key_mapping = self._read_table_with_primary_key_mappnig(request)
                self._update_primary_key_for_table(client_dump, table_key_mapping)
                tables_with_no_primary_keys = get_table_ids_with_no_primary_key_mapping(
                    client_dump
                )
                client_dump.is_primary_key_uploaded = True
                client_dump.save()
                return Response(
                    {
                        "tables_with_no_primary_key": tables_with_no_primary_keys,
                        "success": True,
                    },
                    status=status.HTTP_200_OK,
                )
        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump with dump-id: {dump_id} not exists",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            message = f"Internal server error, {e} for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def get(self, request, client_id: int, dump_id: int):
        try:
            client_dump = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            tables_with_no_primary_keys = get_table_ids_with_no_primary_key_mapping(
                client_dump
            )
            return Response(
                {
                    "tables_with_no_primary_key": tables_with_no_primary_keys,
                    "is_primary_key_uploaded": client_dump.is_primary_key_uploaded,
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump with dump-id: {dump_id}, client-id: {client_id} not exists",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            message = f"Internal server error, {e} for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def get_table_ids_with_no_primary_key_mapping(dump_obj: ClientDataDump):
    all_tables: list[Table] = dump_obj.tables.all()
    no_primary_keys = []
    for table in all_tables:
        if not table.metadata.primary_key:
            no_primary_keys.append((table.table_name, table.id))
        elif len(table.metadata.primary_key.get('primary_key', [])) < 1:
            no_primary_keys.append((table.table_name, table.id))
    return no_primary_keys


def update_primary_keys_using_previous_dumps(client_dump: ClientDataDump):
    for curr_table in client_dump.tables.all():
        any_prev_dump = (
            ClientDataDump.objects.filter(
                client_id=client_dump.client.id,
                dump_date__lt=client_dump.dump_date,
                tables__table_name=curr_table.table_name
            )
            .order_by("-dump_date")
            .first()
        )
        prev_table: Table = any_prev_dump.tables.get(table_name=curr_table.table_name) if any_prev_dump else None
        if prev_table is not None and "primary_key" in prev_table.metadata.primary_key:
            curr_cols = [col['column_name'] for col in curr_table.table_details_for_ui['columns_details']]
            primary_cols = prev_table.metadata.primary_key["primary_key"]
            if set(primary_cols).issubset(set(curr_cols)):
                curr_table.metadata.primary_key["primary_key"] = prev_table.metadata.primary_key["primary_key"]
                curr_table.metadata.save()
