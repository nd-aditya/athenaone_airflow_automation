from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from nd_api.decorator import conditional_authentication
from nd_api_v2.decorator import conditional_authentication
from django.conf import settings

@conditional_authentication
class IncrementalSchedulerTypeView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        try:
            response_json = {
                "success": True,
                "message": "Scheduler configuration found",
                "incremental_process_type": settings.INCREMENTAL_PROCESS_TYPE,
            }
            return Response(response_json, status=status.HTTP_200_OK)
        except Exception as e:
            response_json = {
                "success": False,
                "message": str(e),
            }
            return Response(response_json, status=status.HTTP_500_INTERNAL_SERVER_ERROR)