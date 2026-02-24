import traceback
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Clients
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


# pii_config = {
#     "mask": {
#         "name": "((FirstName))",
#         "dob": "((dob))",
#         "phone": "((phone))",
#         "email": "((email))",
#         "insurnce_id": "((insurnce_id))",
#     },
#     "dob": ["dob"],
#     "replace_values": [{1001: 10001101011}, {"Texas": "((FacilityName))"}],
# }


@conditional_authentication
class PIITablesView(APIView):
    authentication_classes = [IsAuthenticated]

    def _get_columns(self, source_tables: dict):
        listofcols = []
        for tbname, tb_conf in source_tables.items():
            listofcols.extend(
                [list(d.values())[0] for d in tb_conf["required_columns"]]
            )
        return list(set(listofcols))

    def get(self, request, client_id: int):
        try:
            client_obj = Clients.objects.get(id=client_id)
            pii_tables = client_obj.master_db_config["pii_tables"]

            table_details = []
            for table_name, config in pii_tables.items():
                table_details.append(
                    {"table_name": table_name, "columns": self._get_columns(config['source_tables'])}
                )
            response_json = {"tables_details": table_details}
            return Response(response_json, status=status.HTTP_200_OK)
        except Clients.DoesNotExist:
            return Response(
                {"message": f"client-id: {client_id}, does not exists"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
