import pydicom
from neuropacs.models import Patients
from .rules import PatientUUIDDeIdentify, DicomDeidentifyRule, NDTagsRule, RemovePIITagsRule, RemovePrivateTags


class DicomDeIdentifier:
    def __init__(self, dicom_ds: str):
        self.dicom_ds = dicom_ds
        self.client_patient_id = str(dicom_ds.get("PatientID"))

    
    def deidentify(self):
        patient = Patients.objects.get(client_patient_id=self.client_patient_id)
        offset_value = patient.offset_value
        
        rules: list[DicomDeidentifyRule]  = [
            PatientUUIDDeIdentify(self.dicom_ds),
            # DOBDeIdentify(self.dicom_ds),
            # DatesDeIdentify(self.dicom_ds, offset_value),
            # MaskMetadata(self.dicom_ds),
            RemovePIITagsRule(self.dicom_ds),
            RemovePrivateTags(self.dicom_ds),
            NDTagsRule(self.dicom_ds),
        ]
        for rule in rules:
            rule.apply()
        return self.dicom_ds

class OTModalityDeIdentifier:
    def __init__(self, dicom_ds: str):
        self.dicom_ds = dicom_ds
        self.client_patient_id = str(dicom_ds.get("PatientID"))

    def deidentify(self):
        pass