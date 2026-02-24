import subprocess
import tempfile
import os
import traceback
import pydicom
from typing import Union
from pydicom.uid import JPEG2000Lossless
from pydicom.encaps import encapsulate
from django.conf import settings
from typing import TypedDict
from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from django.db import transaction
from worker.models import Task, Chain
from neuropacs.models import PacsClient, PatientInstance, Patients, PatientStudy, PatientSeries
from neuropacs.models.utils import Status
from keycloakauth.utils import IsAuthenticated
from deIdentification.nd_logger import nd_logger
from nd_api.decorator import conditional_authentication
from neuropacs.deidentifier.dicom_deidentifier import DicomDeIdentifier
import pydicom
from pydicom.uid import JPEGLSLossless
import os
from ndwebsocket.utils import (
    broadcast_task_status, 
    broadcast_task_progress, 
    broadcast_task_error,
    save_notification_to_db
)
from ndwebsocket.models import NotificationType, NotificationPriority


class RequestCtx(TypedDict):
    client_name: str
    emr_type: str
    patient_identifier_columns: list[str]


# @conditional_authentication
# class StartDeidPacsDataView(APIView):
#     authentication_classes = [IsAuthenticated]

#     def get(self, request, client_id: int, pacs_client_id: int):
#         try:
#             pacs_client = PacsClient.objects.get(client__id=client_id, id=pacs_client_id)
#             deidentify_pacs_data_task(pacs_client)
            
#             return Response(f"Pushed the deidentification for pacs_client_id: {pacs_client_id}", status=status.HTTP_200_OK)
#         except PacsClient.DoesNotExist as e:
#             return Response(
#                 {"message": "invalid data provided"},
#                 status=status.HTTP_400_BAD_REQUEST,
#             )
#         except Exception as e:
#             nd_logger.error(f"Internal server error: {e}")
#             nd_logger.error(traceback.format_exc())
#             return Response(
#                 {"message": "Internal server error: {e}"},
#                 status=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             )

@conditional_authentication
class StartDeidPacsDataView(APIView):
    authentication_classes = [IsAuthenticated]

    def post(self, request, client_id: int, pacs_client_id: int):
        try:
            hierarchy_type = request.data["hierarchy_type"]
            model_obj = None

            client_patient_id = request.data["client_patient_id"]

            # Broadcast PACS de-identification start
            broadcast_task_status(
                status="started",
                task_name=f"PACS De-identification: {hierarchy_type}",
                message=f"Starting PACS de-identification for {hierarchy_type} hierarchy",
                priority=NotificationPriority.HIGH,
                notification_type=NotificationType.TASK_STATUS,
                data={"client_id": client_id, "pacs_client_id": pacs_client_id, "hierarchy_type": hierarchy_type}
            )

            if hierarchy_type == "pacs_client":
                model_obj = PacsClient.objects.get(
                    id=pacs_client_id,
                    pacs_client__client_id=client_id,
                )

            if hierarchy_type == "patient":
                model_obj = Patients.objects.get(
                    pacs_client__id=pacs_client_id,
                    pacs_client__client__id=client_id,
                    client_patient_id=client_patient_id
                )
            elif hierarchy_type == "study":
                client_study_uid = request.data["client_study_instance_uid"]
                model_obj = PatientStudy.objects.get(
                    patient__pacs_client__id=pacs_client_id,
                    patient__pacs_client__client__id=client_id,
                    patient__client_patient_id=client_patient_id,
                    client_study_instance_uid=client_study_uid
                )
            elif hierarchy_type == "series":
                client_study_uid = request.data["client_study_instance_uid"]
                client_series_uid = request.data["client_series_instance_uid"]
                model_obj = PatientSeries.objects.get(
                    study__patient__pacs_client__id=pacs_client_id,
                    study__patient__pacs_client__client__id=client_id,
                    study__patient__client_patient_id=client_patient_id,
                    study__client_study_instance_uid=client_study_uid,
                    client_series_instance_uid=client_series_uid
                )

            elif hierarchy_type == "instance":
                client_study_uid = request.data["client_study_instance_uid"]
                client_series_uid = request.data["client_series_instance_uid"]
                client_sop_uid = request.data["client_sop_instance_uid"]
                model_obj = PatientInstance.objects.get(
                    series__study__patient__pacs_client__id=pacs_client_id,
                    series__study__patient__pacs_client__client__id=client_id,
                    series__study__patient__client_patient_id=client_patient_id,
                    series__study__client_study_instance_uid=client_study_uid,
                    series__client_series_instance_uid=client_series_uid,
                    client_sop_instance_uid=client_sop_uid
                )
            else:
                # Broadcast invalid hierarchy type error
                broadcast_task_error(
                    task_name="PACS De-identification",
                    error=f"Invalid hierarchy type: {hierarchy_type}",
                    error_code="INVALID_HIERARCHY_TYPE",
                    details={"hierarchy_type": hierarchy_type, "client_id": client_id, "pacs_client_id": pacs_client_id}
                )
                return Response(
                    {"message": "invalid data provided", "hierarchy_type": "instance"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            tasks = deidentify_pacs_data_task(model_obj)
            
            return Response(f"Pushed the deidentification for pacs_client_id: {pacs_client_id}", status=status.HTTP_200_OK)
        except (PacsClient.DoesNotExist, KeyError) as e:
            # Broadcast error notification
            broadcast_task_error(
                task_name="PACS De-identification",
                error=f"Invalid data provided: {str(e)}",
                error_code="INVALID_DATA",
                details={"client_id": client_id, "pacs_client_id": pacs_client_id, "error": str(e)}
            )
            return Response(
                {"message": "invalid data provided", "error": f"{e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            nd_logger.error(f"Internal server error: {e}")
            nd_logger.error(traceback.format_exc())
            
            # Broadcast error notification
            broadcast_task_error(
                task_name="PACS De-identification",
                error=f"Internal server error: {str(e)}",
                error_code="PACS_DEID_ERROR",
                details={"client_id": client_id, "pacs_client_id": pacs_client_id, "error": str(e)}
            )
            
            return Response(
                {"message": "Internal server error: {e}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
    
    # def get(self, request, client_id: int, pacs_client_id: int, client_patient_id: int):
    #     try:
    #         patient = Patients.objects.get(pacs_client__id=pacs_client_id, pacs_client__client__id=client_id, client_patient_id=client_patient_id)
    #         deidentify_pacs_data_task(pacs_client_id)
            
    #         return Response(f"Pushed the deidentification for pacs_client_id: {pacs_client_id}", status=status.HTTP_200_OK)
    #     except PacsClient.DoesNotExist as e:
    #         return Response(
    #             {"message": "invalid data provided"},
    #             status=status.HTTP_400_BAD_REQUEST,
    #         )
    #     except Exception as e:
    #         nd_logger.error(f"Internal server error: {e}")
    #         nd_logger.error(traceback.format_exc())
    #         return Response(
    #             {"message": "Internal server error: {e}"},
    #             status=status.HTTP_500_INTERNAL_SERVER_ERROR,
    #         )


def deidentify_pacs_data_task(model_obj: Union[PacsClient, Patients, PatientStudy, PatientSeries, PatientInstance]):
    # Broadcast task creation start
    model_type = type(model_obj).__name__
 
    instances_to_deid = []
    tasks = []
    with transaction.atomic():
        filter_kwargs = {"deid_status": 0}

        filter_map = {
            PacsClient: "series__study__patient__pacs_client",
            Patients: "series__study__patient",
            PatientStudy: "series__study",
            PatientSeries: "series",
        }

        for model_class, filter_path in filter_map.items():
            if isinstance(model_obj, model_class):
                filter_kwargs[filter_path] = model_obj
                instances_to_deid = PatientInstance.objects.filter(**filter_kwargs)
                break
        else:
            instances_to_deid = [model_obj]

        chain, created = Chain.all_objects.get_or_create(
            reference_uuid=model_obj.get_chain_reference_uuid_for_pacs_inventory()
        )

        if not created:
            chain.revive_and_save()
        for idx, instance in enumerate(instances_to_deid):
            task = Task.create_task(
                fn=deidentify_instance,
                chain=chain,
                arguments={"instance_id": instance.id},
                hooks={"failure": deidentify_instance_failure_hook},
            )
            tasks.append(task)
        task = Task.create_task(
            fn=cleanup_task,
            chain=chain,
            arguments={"chain_id": chain.id},
        )
    return tasks


def cleanup_task(chain_id: int, dependencies: list[dict] = []):
    # Broadcast cleanup start

    chain = Chain.objects.get(id=chain_id)
    with transaction.atomic(savepoint=settings.CREATE_SAVEPOINT_IN_TRANSACTION):
        chain.soft_delete_and_save()

    
def deidentify_instance(instance_id: int, dependencies: list[dict]=[]):
    try:
        instance = PatientInstance.objects.get(id=instance_id)
        dicom_dataset = pydicom.dcmread(instance.original_file_path)
        deidentifier = DicomDeIdentifier(dicom_dataset)
        dicom_dataset = deidentifier.deidentify()
        
        # Broadcast file saving
        save_path = instance.get_deidentified_file_path()
        dicom_dataset = compress_file_if_required(dicom_dataset)
        pydicom.write_file(save_path, dicom_dataset)
        
        # Broadcast completion
        instance.deid_status = Status.COMPLETED
        instance.save()
        
        broadcast_task_status(
            status="completed",
            task_name=f"PACS Instance De-identification: {instance_id}",
            message=f"Successfully de-identified and saved instance {instance_id}",
            priority=NotificationPriority.MEDIUM,
            notification_type=NotificationType.SUCCESS,
            data={"instance_id": instance_id, "save_path": save_path, "sop_uid": getattr(instance, 'client_sop_instance_uid', 'Unknown')}
        )
        
    except Exception as e:
        # Broadcast error notification
        broadcast_task_error(
            task_name=f"PACS Instance De-identification: {instance_id}",
            error=f"Failed to de-identify instance {instance_id}: {str(e)}",
            error_code="INSTANCE_DEID_ERROR",
            details={"instance_id": instance_id, "error": str(e)}
        )
        raise




def compress_file_if_required(dicom_dataset: pydicom.Dataset, 
                            compression_uid: str = JPEGLSLossless) -> pydicom.Dataset:
    try:
        if 'PixelData' not in dicom_dataset:
            nd_logger.info("Skipping compression: No PixelData found")
            return dicom_dataset
        
        if not hasattr(dicom_dataset, 'file_meta') or dicom_dataset.file_meta is None:
            nd_logger.warning("No file_meta found, creating default")
            dicom_dataset.file_meta = pydicom.Dataset()
            dicom_dataset.file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
        
        if hasattr(dicom_dataset.file_meta, 'TransferSyntaxUID'):
            if getattr(dicom_dataset.file_meta.TransferSyntaxUID, "is_compressed", False):
                nd_logger.info(f"Skipping compression: Already compressed with {dicom_dataset.file_meta.TransferSyntaxUID}")
                return dicom_dataset
        
        original_size = len(dicom_dataset.PixelData) if hasattr(dicom_dataset, 'PixelData') else 0
        
        dicom_dataset.compress(compression_uid)
        
        compressed_size = len(dicom_dataset.PixelData) if hasattr(dicom_dataset, 'PixelData') else 0
        if original_size > 0 and compressed_size > 0:
            ratio = original_size / compressed_size
            nd_logger.info(f"Compression successful: {original_size} -> {compressed_size} bytes (ratio: {ratio:.2f}:1)")
        
        return dicom_dataset
        
    except Exception as e:
        nd_logger.error(f"Compression failed: {e}")
        return dicom_dataset  # Return original dataset on failure
# def compress_file_if_required(dicom_dataset: pydicom.Dataset) -> pydicom.Dataset:
    
#     # Check if already compressed
#     if getattr(dicom_dataset.file_meta.TransferSyntaxUID, "is_compressed", False):
#         return dicom_dataset

#     # Skip if no pixel data
#     if 'PixelData' not in dicom_dataset:
#         return dicom_dataset

#     # Use temporary files to call DCMTK
#     with tempfile.NamedTemporaryFile(suffix=".dcm", delete=False) as temp_in:
#         dicom_dataset.save_as(temp_in.name)
#         temp_in_path = temp_in.name

#     temp_out_path = temp_in_path.replace(".dcm", "_compressed.dcm")

#     try:
#         # Call DCMTK dcmcjpeg for JPEG2000 Lossless
#         # +oj: encode to JPEG2000 Lossless
#         # subprocess.run([
#         #     "dcmcjpeg",
#         #     "+oj",                # JPEG2000 Lossless
#         #     temp_in_path,
#         #     temp_out_path
#         # ], check=True)

#         subprocess.run([
#             "dcmcjpeg",
#             "+e",                # Encode to JPEG
#             "+lossless",         # Use lossless mode
#             temp_in_path,
#             temp_out_path
#         ], check=True)

#         # Read compressed DICOM back into memory
#         compressed_ds = pydicom.dcmread(temp_out_path)

#     finally:
#         # Clean up temp files
#         if os.path.exists(temp_in_path):
#             os.remove(temp_in_path)
#         if os.path.exists(temp_out_path):
#             os.remove(temp_out_path)

#     return compressed_ds

# def compress_file_if_required(dicom_dataset: pydicom.Dataset) -> pydicom.Dataset:
#     if dicom_dataset.file_meta.TransferSyntaxUID.is_compressed:
#         return dicom_dataset

#     if 'PixelData' not in dicom_dataset:
#         return dicom_dataset

#     dicom_dataset.compress(JPEG2000Lossless)
#     return dicom_dataset

def deidentify_instance_failure_hook(chain: Chain):
    # Broadcast failure notification
    broadcast_task_error(
        task_name=f"PACS Instance De-identification: Chain {chain.id}",
        error=f"De-identification task failed in chain {chain.id}",
        error_code="INSTANCE_DEID_FAILURE",
        details={"chain_id": chain.id, "reference_uuid": chain.reference_uuid}
    )
