from django.conf import settings
from typing import Dict, Any
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from nd_api_v2.models.incremental_queue import IncrementalQueue, QueueStatus
from nd_api_v2.models.table_details import Table, Status
from nd_api_v2.decorator import conditional_authentication

RequestCtx = Dict[str, Any]


def get_status_string(status_code: int) -> str:
    """Convert status code to string"""
    status_map = {
        Status.NOT_STARTED: "Not Started",
        Status.IN_PROGRESS: "In Progress",
        Status.COMPLETED: "Completed",
        Status.FAILED: "Failed",
        Status.INTERUPTED: "Interrupted",
    }
    return status_map.get(status_code, "Unknown")


def get_gcp_status_string(status_code: int) -> str:
    """Convert GCP status code to string"""
    status_map = {
        Status.NOT_STARTED: "Not Started",
        Status.IN_PROGRESS: "In Process",
        Status.COMPLETED: "Moved",
        Status.FAILED: "Failed",
        Status.INTERUPTED: "Interrupted",
    }
    return status_map.get(status_code, "Unknown")


@conditional_authentication
class MonitoringQueueTablesView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, queue_name: str):
        """Get all tables and their statuses for a specific queue"""
        try:
            # Get all queues with this queue_name
            queues = IncrementalQueue.objects.filter(queue_name=queue_name).order_by('-created_at')
            
            if not queues.exists():
                return Response({
                    "message": f"Queue '{queue_name}' not found",
                    "success": False,
                    "tables": [],
                    "total_tables": 0,
                    "deid_completed": 0,
                    "failed_tables": 0,
                    "status_summary": {
                        "deid": {},
                        "qc": {},
                        "embd": {},
                        "gcp": {},
                    }
                }, status=status.HTTP_404_NOT_FOUND)

            # Get all tables for this queue using the reverse relationship
            # Table has incremental_queue ForeignKey with related_name="table"
            # So we can access tables via queue.table.all() or query Table directly
            tables = Table.objects.filter(incremental_queue__queue_name=queue_name).select_related(
                'metadata', 'deid', 'qc', 'embd', 'gcp', 'incremental_queue'
            ).prefetch_related('metadata')
            
            status_keys = ["Not Started", "In Progress", "Completed", "Interrupted", "Failed"]
            gcp_status_keys = ["Not Started", "In Process", "Moved", "Interrupted", "Failed"]

            def init_status_counter(keys):
                return {key: 0 for key in keys}

            status_summary = {
                "deid": init_status_counter(status_keys),
                "qc": init_status_counter(status_keys),
                "embd": init_status_counter(status_keys),
                "gcp": init_status_counter(gcp_status_keys),
            }

            tables_data = []
            for table in tables:
                metadata = table.metadata
                
                # Get run range
                start_value = getattr(table, 'nd_auto_increment_start_value', 0)
                end_value = getattr(table, 'nd_auto_increment_end_value', 0)
                run_range = f"{start_value}-{end_value}" if end_value > 0 else f"{start_value}-0"
                
                # Get statuses
                deid_status = get_status_string(table.deid.deid_status) if table.deid else "Not Started"
                emid_status = "Not Started"  # EMID status might need to be added to the model
                qc_status = get_status_string(table.qc.qc_status) if table.qc else "Not Started"
                embd_status = get_status_string(table.embd.embd_stats) if table.embd else "Not Started"
                gcp_status = get_gcp_status_string(table.gcp.cloud_uploaded) if table.gcp else "Not Started"
                
                # Determine pipeline state (overall status)
                pipeline_state = "Pending"
                if deid_status == "Failed" or qc_status == "Failed" or embd_status == "Failed" or gcp_status == "Failed":
                    pipeline_state = "Failed"
                elif deid_status == "Completed" and qc_status == "Completed" and embd_status == "Completed" and gcp_status == "Moved":
                    pipeline_state = "Completed"
                elif deid_status == "In Progress" or qc_status == "In Progress" or embd_status == "In Progress" or gcp_status == "In Process":
                    pipeline_state = "In Progress"
                elif deid_status == "Interrupted" or qc_status == "Interrupted" or embd_status == "Interrupted" or gcp_status == "Interrupted":
                    pipeline_state = "Interrupted"
                
                status_summary["deid"][deid_status] = status_summary["deid"].get(deid_status, 0) + 1
                status_summary["qc"][qc_status] = status_summary["qc"].get(qc_status, 0) + 1
                status_summary["embd"][embd_status] = status_summary["embd"].get(embd_status, 0) + 1
                status_summary["gcp"][gcp_status] = status_summary["gcp"].get(gcp_status, 0) + 1

                tables_data.append({
                    "table_id": table.id,
                    "table_name": metadata.table_name if metadata else "N/A",
                    "run_range": run_range,
                    "deid_status": deid_status,
                    "emid_status": emid_status,
                    "qc_status": qc_status,
                    "embd_status": embd_status,
                    "gcp_status": gcp_status,
                    "pipeline_state": pipeline_state,
                })

            # Calculate summary statistics
            total_tables = len(tables_data)
            deid_completed = len([t for t in tables_data if t["deid_status"] == "Completed"])
            failed_tables = len([t for t in tables_data if t["pipeline_state"] == "Failed"])

            return Response({
                "queue_name": queue_name,
                "tables": tables_data,
                "total_tables": total_tables,
                "deid_completed": deid_completed,
                "failed_tables": failed_tables,
                "status_summary": status_summary,
                "success": True
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@conditional_authentication
class MonitoringQueuesListView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request):
        """Get list of available queue names for the dropdown"""
        try:
            # Get distinct queue names
            queue_names = IncrementalQueue.objects.values_list('queue_name', flat=True).distinct().order_by('-queue_name')
            
            return Response({
                "queue_names": list(queue_names),
                "success": True
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({
                "message": "Internal server error",
                "success": False,
                "error": str(e) if settings.DEBUG else None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

