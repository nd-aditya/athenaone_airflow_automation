# import random
# import traceback
# from nd_api.models import ClientDataDump, Table
# from rest_framework.views import APIView
# from rest_framework.response import Response
# from rest_framework import status
# from keycloakauth.utils import IsAuthenticated
# from deIdentification.nd_logger import nd_logger
# from nd_api.decorator import conditional_authentication


# @conditional_authentication
# class ViewTableDataView(APIView):
#     authentication_classes = [IsAuthenticated]

#     def get(self, request, table_id: int):
#         try:
#             row_numbers = request.GET.get("rows", 10)
#             try:
#                 row_numbers = int(row_numbers)
#             except (TypeError, ValueError):
#                 row_numbers = 10

#             table_obj = Table.objects.get(id=table_id)
#             client_dump: ClientDataDump = table_obj.dump
#             connection = client_dump.get_source_db_connection()
#             try:
#                 offset = random.randrange(1, table_obj.rows_count -  row_numbers)
#             except:
#                 offset = 0
#             rows = connection.get_rows(table_obj.table_name, row_numbers, offset)
#             return Response(
#                 {"rows": rows, "table_name": table_obj.table_name, "table_id": table_obj.id},
#                 status=status.HTTP_200_OK
#             )
#         except Exception as e:
#             message = f"ViewTableDataView.get: Internal server error : {e}, for user: {request.user}, table_id: {table_id}"
#             nd_logger.error(message)
#             nd_logger.error(traceback.format_exc())
#             return Response(
#                 message,
#                 status=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             )
