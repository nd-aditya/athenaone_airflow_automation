import traceback
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Clients
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


class RequestCtx(TypedDict):
    client_name: str
    emr_type: str
    patient_identifier_columns: list[str]


@conditional_authentication
class ClientView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request):
        try:
            data: RequestCtx = request.data
            emr_type = data["emr_type"]
            patient_identifier = data['patient_identifier_columns']
            client_obj, created = Clients.objects.get_or_create(
                client_name=data["client_name"]
            )
            if not created:
                return Response(
                    {
                        "message": f"client: {data['client_name']}, already exists in the database, please choose different name"
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            client_obj.emr_type = emr_type
            client_obj.patient_identifier_columns = patient_identifier
            client_obj.save()
            nd_logger.info(f"client registered successfully by user {request.user}")
            return Response(
                {"message": "client registered successfully", "client_id": client_obj.id, "client_name": client_obj.client_name}, status=status.HTTP_200_OK
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request):
        try:
            all_clients = Clients.objects.all()
            nd_logger.info(f"total client found {all_clients.count()}")
            response_output = []
            for client in all_clients:
                dumps_count = client.dumps.count() 
                response_output.append(
                    {
                        "client_id": client.id,
                        "client_name": client.client_name,
                        "emr_type": client.emr_type,
                        "client_presetup_config_configured": client.client_presetup_config_configured,
                        "presetup_remarks": client.presetup_remarks,
                        "patient_identifier_columns": client.patient_identifier_columns,
                        "created_at": client.created_at,
                    }
                )
            return Response(response_output, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


@conditional_authentication
class ClientUpdateView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int):
        try:
            try:
                client_obj = Clients.objects.get(id=client_id)
            except Clients.DoesNotExist:
                return Response(
                    {"message": f"client-id: {client_id}, does not exists"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            response_json = {
                "client_id": client_obj.id,
                "client_name": client_obj.client_name,
                "emr_type": client_obj.emr_type,
                "config": client_obj.config,
                "patient_identifier_columns": client_obj.patient_identifier_columns,
                "mapping_db_config": client_obj.mapping_db_config,
                "master_db_config": client_obj.master_db_config,
                "client_presetup_config_configured": client_obj.client_presetup_config_configured,
                "created_at": client_obj.created_at
            }
            return Response(
                response_json,
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )