import os
import pydicom
from typing import TypedDict
from neuropacs.models import Patients, PatientStudy, PatientSeries, PatientInstance, PacsClient
from neuropacs.models.utils import Status
from neuropacs.constants import DICOMTags
from core.dbPkg.mapping_loader import MappingDb
from deIdentification.nd_logger import nd_logger


class PACSDirHandlerConfig(TypedDict):
    dir_path: str


class PACSDirectoryHandler:
    def __init__(self, config: PACSDirHandlerConfig, pacs_client_id: int):
        self.dir_path: str = config["dir_path"]
        self.pacs_client_obj = PacsClient.objects.get(id=pacs_client_id)
        
        # mapping_db_config = self.pacs_client_obj.client.get_mapping_db_config()
        # self.mapping_db = MappingDb(mapping_db_config)


    def get_all_files(self) -> list[str]:
        if not os.path.isdir(self.dir_path):
            raise FileNotFoundError(f"Directory not found: {self.dir_path}")
        
        dcm_files = []
        for root, _, files in os.walk(self.dir_path):
            for f in files:
                # if f.lower().endswith(".dcm"):
                dcm_files.append(os.path.join(root, f))
                # if f.lower().endswith(".dcm"):
                #     dcm_files.append(os.path.join(root, f))
        return dcm_files

    def register_files(self) -> list[str]:
        all_files = self.get_all_files()
        for file in all_files:
            try:
                dicom_dataset = pydicom.dcmread(file, stop_before_pixels=True)
            except Exception as e:
                nd_logger.info(f"not able to read dicom file, looks like not a dicom file: {file}")
                continue
            client_patient_id=dicom_dataset.get(DICOMTags.PATIENT_ID, None)
            patient, patient_created = Patients.objects.get_or_create(
                client_patient_id=client_patient_id,
                pacs_client=self.pacs_client_obj
            )
            # if not patient.nd_patient_id:
            #     nd_patient_id = self.mapping_db.get_nd_patient_id(client_patient_id, self.pacs_client_obj.patient_identifier_type)
            #     patient.nd_patient_id = nd_patient_id
            study, study_created = PatientStudy.objects.get_or_create(
                patient=patient,
                client_study_instance_uid=dicom_dataset.get(DICOMTags.STUDY_INSTANCE_UID, None),
            )
            series, series_created = PatientSeries.objects.get_or_create(
                study=study,
                client_series_instance_uid=dicom_dataset.get(DICOMTags.SERIES_INSTANCE_UID, None),
            )
            instance = PatientInstance.objects.create(
                series=series,
                client_sop_instance_uid=dicom_dataset.get(DICOMTags.SOP_INSTANCE_UID, None),
            )
            instance.original_file_path = file
            instance.save()
            series.total_instances_count += 1
            series.cloud_uploaded = False
            series.deid_status = Status.IN_PROGRESS if series.deid_status == Status.IN_PROGRESS else Status.NOT_STARTED
            series.save()
            if series_created:
                study.total_series_count += 1
            study.deid_status = Status.IN_PROGRESS if study.deid_status == Status.IN_PROGRESS else Status.NOT_STARTED
            study.save()
            if study_created:
                patient.total_study_count += 1
            patient.deid_status = Status.IN_PROGRESS if patient.deid_status == Status.IN_PROGRESS else Status.NOT_STARTED
            patient.save()