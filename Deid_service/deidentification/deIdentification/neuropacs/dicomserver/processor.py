import os
import logging
import string
import random as std_random
import pydicom
from django.conf import settings
from neuropacs.deidentifier.dicom_deidentifier import DicomDeIdentifier


logger = logging.getLogger(__name__)

def random_string(size=6, chars=string.ascii_lowercase + string.digits):
    return "".join(std_random.choice(chars) for _ in range(size))


def get_save_path(dataset):
    nd_patient_id = str(dataset.get("PatientID"))
    studyInstanceUID = str(dataset.get("StudyInstanceUID"))
    seriesInstanceUID = str(dataset.get("SeriesInstanceUID"))
    sopInstanceUID = str(dataset.get("SOPInstanceUID"))

    save_path = os.path.join(settings.PACS_DATA_SAVE_PATH, nd_patient_id)
    save_path = os.path.join(save_path, studyInstanceUID)
    save_path = os.path.join(save_path, seriesInstanceUID)
    os.makedirs(save_path, exist_ok=True)

    save_path = os.path.join(save_path, f"{sopInstanceUID}.dcm")
    return save_path

def process_dicom_store(sender, dataset: pydicom.Dataset):
    sop_id = dataset.SOPInstanceUID
    logger.info('Processing dataset with sopID: %s', sop_id)
    deidentifier = DicomDeIdentifier(dataset)
    dataset = deidentifier.deidentify()
    save_path = get_save_path(dataset)
    pydicom.write_file(save_path, dataset)