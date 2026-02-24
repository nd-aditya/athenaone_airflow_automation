from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from nd_api.decorator import conditional_authentication
from nd_api.models import Clients
from deIdentification.nd_logger import nd_logger
from core.process_df.rules import Rules


@conditional_authentication
class DEIDRulesView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int):
        try:
            client_obj = Clients.objects.get(id=client_id)
            pid_cols = client_obj.patient_identifier_columns
            all_rules = [
                Rules.APPOINTMENT_ID.value,
                Rules.DATE_OFFSET.value,
                Rules.STATIC_OFFSET.value,
                Rules.DOB.value,
                Rules.MASK.value,
                Rules.NOTES.value,
                Rules.GENERIC_NOTES.value,
                Rules.ZIP_CODE.value,
                Rules.ENCOUNTER_ID.value,
            ]
            for pid_col in pid_cols:
                all_rules.append(f"PATIENT_{pid_col}".upper())

            return Response(
                {"rules": all_rules, "success": True}, status=status.HTTP_200_OK
            )
        except Clients.DoesNotExist:
            error_messgae = f""
            nd_logger.info(error_messgae)
            return Response(
                {"error_message": error_messgae, "success": False},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            error_messgae = f"error faced while getting the rules for the client: {client_id}, user: {request.user}"
            nd_logger.info(error_messgae)
            return Response(
                {"error_message": error_messgae, "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
