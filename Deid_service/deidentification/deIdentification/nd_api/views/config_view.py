import traceback
from typing import TypedDict
from rest_framework.views import APIView
from keycloakauth.utils import IsAuthenticated
from django.conf import settings
from rest_framework import status
from rest_framework.response import Response
from nd_api.models import Table, Status, ClientDataDump, Clients
from deIdentification.nd_logger import nd_logger
from qc_package.scanner import DbScanner
from nd_api.decorator import conditional_authentication


class ClientRunConfig(TypedDict, total=False):
    admin_connection_str: str
    nd_patient_start_value: str
    default_offset_value: str


class ClientConfig(TypedDict):
    master_config: dict
    mapping_config: dict
    client_run_config: ClientRunConfig
    ovewrite_client_presetup_configuration: bool


class ClientDumpConfig(TypedDict):
    pii_config: dict
    secondary_config: dict
    global_config: dict

    qc_config: dict
    dump_run_config: dict


class ConfigRequestCtx(TypedDict, total=False):
    client_config: ClientConfig
    dump_config: ClientDumpConfig


@conditional_authentication
class ConfigsView(APIView):
    authentication_classes = [IsAuthenticated]

    def _save_client_config(
        self, client_config: ClientConfig, dump_obj: ClientDataDump
    ):
        client_obj = dump_obj.client
        dump_counts_for_client = client_obj.dumps.count()
        ovewrite_client_presetup_config = client_config.get(
            "ovewrite_client_presetup_configuration", False
        )
        if dump_counts_for_client > 2 and (not ovewrite_client_presetup_config):
            nd_logger.info("not doining anything already presetup done")
            return
        client_presetup_config_configured = True
        things_not_configured_yet = []

        if "client_run_config" in client_config:
            client_obj.config.update(client_config["client_run_config"])
        else:
            client_presetup_config_configured = False
            things_not_configured_yet.append("client_run_config")
        if "mapping_config" in client_config:
            client_obj.mapping_db_config.update(client_config["mapping_config"])
        else:
            client_presetup_config_configured = False
            things_not_configured_yet.append("mapping_config")
        if "master_config" in client_config:
            client_obj.master_db_config.update(client_config["master_config"])
        else:
            client_presetup_config_configured = False
            things_not_configured_yet.append("master_config")
        client_obj.client_presetup_config_configured = client_presetup_config_configured
        error_message = ",".join(things_not_configured_yet)
        client_obj.presetup_remarks["error"] = f"{error_message} not configured yet"
        client_obj.save()

    def put(self, request, client_id: int, dump_id: int):
        try:
            data: ConfigRequestCtx = request.data
            try:
                dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            except ClientDataDump.DoesNotExist:
                return Response(
                    {
                        "message": f"dump not exists with dump-id: {dump_id}, client-id: {client_id}",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            client_obj: Clients = dump_obj.client
            self._save_client_config(data.get("client_config", {}), dump_obj)
            client_obj.refresh_from_db()
            dump_config: ClientDumpConfig = data.get("dump_config", {})
            if "pii_config" in dump_config:
                dump_obj.pii_config.update(dump_config["pii_config"])
            if "secondary_config" in dump_config:
                dump_obj.secondary_config = dump_config["secondary_config"]
            if "global_config" in dump_config:
                dump_obj.global_config = dump_config["global_config"]
            if "qc_config" in dump_config:
                dump_obj.qc_config.update(dump_config["qc_config"])
            if "dump_run_config" in dump_config:
                dump_obj.run_config.update(dump_config["dump_run_config"])

            dump_obj.save()
            return Response(
                {"message": "successfully updated config", "success": True},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def get(self, request, client_id: int, dump_id: int):
        try:
            dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            client_obj: Clients = dump_obj.client
            config = {
                "client_config": {
                    "master_config": client_obj.master_db_config,
                    "mapping_config": client_obj.mapping_db_config,
                    "client_run_config": client_obj.config,
                },
                "dump_config": {
                    "qc_config": _get_qc_config(dump_obj),
                    "global_config": dump_obj.global_config,
                    "secondary_config": dump_obj.secondary_config,
                    "pii_config": dump_obj.pii_config,
                    "dump_run_config": dump_obj.run_config,
                },
            }
            return Response(config, status=status.HTTP_200_OK)

        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump not exists with dump-id: {dump_id}, client-id: {client_id}",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            nd_logger.info(f"failing in config-veiw call, traceback: {traceback.format_exc()}")
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def _get_qc_config(dump_obj: ClientDataDump):
    default_config = {
        "PATIENT_IDENTIFIER": {"prefix_value": "1001000", "length_of_value": 15},
        "ENCOUNTER_IDENTIFIER": {"prefix_value": "1001000", "length_of_value": 19},
        "APPOINTMENT_IDENTIFIER": {"prefix_value": "1001000", "length_of_value": 19},
    }
    if dump_obj.qc_config  in [{}, None]:
        return default_config
    return dump_obj.qc_config


@conditional_authentication
class ReUseProcessingConfigView(APIView):
    authentication_classes = [IsAuthenticated]

    def get(self, request, client_id: int, dump_id: int):
        try:
            curr_dump_obj = ClientDataDump.objects.get(id=dump_id, client__id=client_id)
            total_dump_count = ClientDataDump.objects.filter(client_id=client_id).count()
            if total_dump_count < 2:
                return Response(
                    {
                        "message": f"previous dump not exists for dump-id: {dump_id}, client-id: {client_id}",
                        "success": False,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            previous_dump = ClientDataDump.objects.filter(client__id=client_id, dump_date__lt=curr_dump_obj.dump_date).last()
            curr_dump_obj.qc_config = previous_dump.qc_config
            curr_dump_obj.global_config = previous_dump.global_config
            curr_dump_obj.secondary_config = previous_dump.secondary_config
            curr_dump_obj.pii_config = previous_dump.pii_config
            curr_dump_obj.run_config = previous_dump.run_config
            curr_dump_obj.save()
            return Response(
                {
                    "message": f"reused the processing config for: dump-id: {dump_id}, client-id: {client_id}",
                    "success": True,
                },
                status=status.HTTP_200_OK,
            )
        except ClientDataDump.DoesNotExist:
            return Response(
                {
                    "message": f"dump not exists with dump-id: {dump_id}, client-id: {client_id}",
                    "success": False,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            message = f"Internal server error: {e}, user: {request.user}, client-id: {client_id}, dump-id: {dump_id}"
            nd_logger.error(message)
            nd_logger.error(traceback.format_exc())
            return Response(
                {"message": "Internal server error", "success": False},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
