
import traceback
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import ClientDataDump, Status
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from worker.models.task import Task, ComputationStatus


@conditional_authentication
class DumpDashboardView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, dump_id: int):
        try:
            dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            response_json = {
                "deid_status": {
                    "not_started": dump_obj.tables.filter(deid__deid_status=Status.NOT_STARTED).count(),
                    "in_process": dump_obj.tables.filter(deid__deid_status=Status.IN_PROGRESS).count(),
                    "completed": dump_obj.tables.filter(deid__deid_status=Status.COMPLETED).count(),
                    "failed": dump_obj.tables.filter(deid__deid_status=Status.FAILED).count()
                },
                "qc_status": {
                    "not_started": dump_obj.tables.filter(qc__qc_status=Status.NOT_STARTED).count(),
                    "in_process": dump_obj.tables.filter(qc__qc_status=Status.IN_PROGRESS).count(),
                    "passed": dump_obj.tables.filter(qc__qc_status=Status.COMPLETED).count(),
                    "failed": dump_obj.tables.filter(qc__qc_status=Status.FAILED).count(),
                },
                "gcp_status": {
                    "moved": dump_obj.tables.filter(gcp__cloud_uploaded=Status.COMPLETED).count(),
                    "not_moved": dump_obj.tables.filter(gcp__cloud_uploaded=Status.NOT_STARTED).count(),
                    "failed": dump_obj.tables.filter(gcp__cloud_uploaded=Status.FAILED).count(),
                    "in_process": dump_obj.tables.filter(gcp__cloud_uploaded=Status.IN_PROGRESS).count(),
                },
                "task_status": {
                    "pending": Task.objects.filter(status=ComputationStatus.NOT_STARTED).count(),
                    "failed": Task.objects.filter(status=ComputationStatus.FAILURE).count(),
                    "completed": Task.objects.filter(status=ComputationStatus.COMPLETED).count(),
                    "running": Task.objects.filter(status=ComputationStatus.PROCESSING).count(),
                },
                "pacs_status": {
                    "total_patients": 0,
                    "total_studies": 0,
                    "total_files": 0,
                    "deidentified_files": 0,
                    "deid_failed_files_count": 0,
                    "files_uploaded_to_gcp": 0
                },
                "total_tables_count": dump_obj.tables.all().count(),
            }
            return Response(response_json, status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
