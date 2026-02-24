import traceback
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from neuropacs.models import PacsClient
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from neuropacs.datareceiver import PACS_HANDLER
from worker.models import Task, Chain


@conditional_authentication
class PacsInventoryView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, pacs_client_id: int):
        try:
            pacs_client = PacsClient.objects.get(client__id=client_id, id=pacs_client_id)
            chain, created = Chain.all_objects.get_or_create(
                reference_uuid=pacs_client.get_chain_reference_uuid_for_pacs_inventory()
            )
            if not created:
                chain.revive_and_save()
            task = Task.create_task(
                fn=create_pacs_inventory_task,
                chain=chain,
                arguments={"pacs_client_id": pacs_client_id},
                hooks={"failure": de_identification_failure_hook_for_table},
            )
            return Response(f"successfully created inventory task with taskid: {task.id}", status=status.HTTP_200_OK)
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

def create_pacs_inventory_task(pacs_client_id: int, dependencies: list[dict] = []):
    pacs_client = PacsClient.objects.get(id=pacs_client_id)
    handler_cls = PACS_HANDLER[pacs_client.handler_type]
    handler = handler_cls(pacs_client.run_config, pacs_client.id)
    handler.register_files()
    pacs_client.inventory_creation_done = True
    pacs_client.save()
    return {"success": True}


def de_identification_failure_hook_for_table(chain: Chain):
    pass