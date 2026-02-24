import traceback
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from nd_api.models import ClientDataDump, Table, Status
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication


@conditional_authentication
class TablesDetailsForUIView(APIView):
    authentication_classes = [IsAuthenticated]

    def _safe_qc_result_response(self, qc_result: dict):
        nd_auto_ids = qc_result.get('final_qc_result', {}).get('failure_nd_auto_incr_ids', [])
        str_nd_auto_ids = [str(_id) for _id in nd_auto_ids]
        if "final_qc_result" not in qc_result:
            qc_result['final_qc_result'] = {}
        qc_result['final_qc_result']['failure_nd_auto_incr_ids'] = str_nd_auto_ids
        return qc_result
    
    def get(self, request, client_id: int, dump_id: int):
        try:
            try:
                dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            except ClientDataDump.DoesNotExist:
                return Response(
                    {"message": "dump_id does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            tables_objs = dump_obj.tables.all()
            tables_details = []
            for table_obj in tables_objs:
                table_obj: Table = table_obj
                tables_details.append(
                    {
                        "table_name": table_obj.table_name,
                        "table_id": table_obj.id,
                        "is_phi_marking_done": table_obj.is_phi_marking_done,
                        "deid": {
                            "status": table_obj.deid.deid_status,
                            "remarks": table_obj.deid.failure_remarks 
                        },
                        "qc": {
                            "status": table_obj.qc.qc_status,
                            "remarks": self._safe_qc_result_response(table_obj.qc.qc_result)
                        },
                        "gcp": {
                            "status": table_obj.gcp.cloud_uploaded,
                            "remarks": table_obj.gcp.failure_remarks 
                        },
                        "embd": {
                            "status": table_obj.embd.embd_stats,
                            "remarks": table_obj.embd.failure_remarks
                        },
                    }
                )
            return Response(tables_details, status=status.HTTP_200_OK)
        except Exception as e:
            message = f"TablesForUIView: Internal server error : {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

@conditional_authentication
class TablesConfigForUIView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, table_id: int):
        try:
            try:
                table_obj = Table.objects.get(id=table_id)
            except Table.DoesNotExist:
                return Response(
                    {"message": f"table_id: {table_id} does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            response_json = {
                "table_name": table_obj.table_name,
                "table_id": table_obj.id,
                "is_phi_marking_done": table_obj.is_phi_marking_done,
                "table_details_for_ui": table_obj.table_details_for_ui,
                "rows_count": table_obj.rows_count,
                "deid_status": table_obj.deid.deid_status,
                "qc_status": table_obj.qc.qc_status,
                "embedding_status": table_obj.embd.embd_stats,
                "is_cloud_moved": table_obj.gcp.cloud_uploaded,
            }
            return Response(
                response_json,
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"TablesConfigForUIView.get: Internal server error : {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def post(self, request, table_id: int):
        try:
            data = request.data
            try:
                table_details_obj = Table.objects.get(id=table_id)
            except Table.DoesNotExist:
                return Response(
                    {"message": "table_id does not exist", "success": False},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if table_details_obj.is_phi_marking_locked:
                return Response(
                    {
                        "message": "Failed, PHI Marking is locked, cant save the config",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if _is_diff_present_in_dict(table_details_obj.table_details_for_ui, data):
                table_details_obj.table_details_for_ui = data
                table_details_obj.is_phi_marking_done = True
                # table_details_obj.is_phi_marking_locked = False
                table_details_obj.table_status = Status.NOT_STARTED
            table_details_obj.save()
            return Response(
                {"message": "Table details for UI updated successfully", "success": False},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"TablesConfigForUIView.post: Internal server error: {e}, for user : {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                message,
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

def _is_diff_present_in_dict(dict1, dict2):
    return dict1 != dict2
