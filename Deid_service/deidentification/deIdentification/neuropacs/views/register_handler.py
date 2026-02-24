import traceback
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Clients
from neuropacs.models import PacsClient
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


class RequestCtx(TypedDict):
    client_name: str
    emr_type: str
    patient_identifier_columns: list[str]


@conditional_authentication
class RegisterPACSHandlerView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, client_id: int):
        try:
            data: RequestCtx = request.data
            client_obj = Clients.objects.get(id=client_id)
            pacs_client = PacsClient.objects.create(
                client=client_obj,
                handler_type=data["handler_type"],
                register_date=data['register_date'],
                patient_identifier_type=data['patient_identifier_type'],
                run_config=data['run_config']
            )
            nd_logger.info(f"pacs-client: {pacs_client.id} registered successfully by user {request.user}")
            return Response(
                {"message": "pacs-client registered successfully", "pacs_client": pacs_client.id}, status=status.HTTP_200_OK
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request, client_id: int):
        try:
            client_obj = Clients.objects.get(id=client_id)
            response_output = []
            for pacs_client in PacsClient.objects.filter(client=client_obj):
                response_output.append(
                    {
                        "pacsclient_id": pacs_client.id,
                        "handler_type": pacs_client.handler_type,
                        'patient_identifier_type': pacs_client.patient_identifier_type,
                        "register_date": pacs_client.register_date,
                        "created_at": pacs_client.created_at,
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
class GetPacsClientView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, pacs_client_id: int):
        try:
            pacs_client = PacsClient.objects.get(client__id=client_id, id=pacs_client_id)
            response_output = {
                "client_id": pacs_client.id,
                "handler_type": pacs_client.handler_type,
                "register_date": pacs_client.register_date,
                "run_config": pacs_client.run_config,
                "patient_identifier_type": pacs_client.patient_identifier_type,
                "inventory_creation_done": pacs_client.inventory_creation_done
            }
            return Response(response_output, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
