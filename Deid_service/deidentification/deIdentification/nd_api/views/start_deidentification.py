import traceback
from nd_api.models import ClientDataDump
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from worker.models import Task, Chain
from nd_api.decorator import conditional_authentication
from portal.alerts import alert_sender
from tqdm import tqdm
from .de_identification_task import create_deidentification_task



@conditional_authentication
class StartDeIdentification(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, dump_id: int):
        try:
            dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=dump_obj.get_chain_reference_uuid()
            )
            if not created:
                chain.revive_and_save()
            task = Task.create_task(
                fn=start_deidentification_for_dump,
                chain=chain,
                arguments={"dump_id": dump_id},
            )
            return Response(f"created de-identification tasks: task-id: {task.id}", status=status.HTTP_200_OK)
        except Exception as e:
            message = f"Internal server error: {e}, for user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def start_deidentification_for_dump(dump_id: int, dependencies: list[Task] = []):
    try:
        dump_obj = ClientDataDump.objects.get(id=dump_id)
        for table_obj in tqdm(dump_obj.tables.all(), "pushing tables for de-identifiation"):
            create_deidentification_task(table_obj)
    except Exception as e:
        prepare_message = {
            "alert_type": "Failed in Starting deIdentification for whole dump",
            "traceback": traceback.format_exc(),
            "dump_identifier": f"{dump_id} - {dump_obj.dump_name}",
            "client_identifier": f"{dump_obj.client.id} - {dump_obj.dump_name}"
        }
        alert_sender.send_message(prepare_message)

