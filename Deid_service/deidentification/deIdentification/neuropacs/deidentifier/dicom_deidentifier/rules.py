import pydicom
from pydicom.uid import generate_uid
from django.db import transaction
from datetime import datetime
from pydicom.tag import Tag
from neuropacs.models import PatientInstance
from .utils import apply_offset_on_date, NdTags


class DicomDeidentifyRule:

    def __init__(self, dicom_ds):
        self.dicom_ds = dicom_ds

    def apply(self):
        raise NotImplementedError()

class PatientUUIDDeIdentify:
    PATIENT_ID_TAG = "PatientID"

    def __init__(self, dicom_ds: pydicom.Dataset):
        self.dicom_ds = dicom_ds

    def apply(self):
        sop_uid = self.dicom_ds.get("SOPInstanceUID")

        with transaction.atomic():
            # Lock instance row and fetch related objects
            instance = (
                PatientInstance.objects
                .select_for_update()
                .select_related("series__study__patient")
                .get(client_sop_instance_uid=sop_uid)
            )
            series = instance.series
            study = series.study
            patient = study.patient

            # --- Study UID ---
            if not study.nd_study_instance_uid:
                study.nd_study_instance_uid = generate_uid()
                study.save(update_fields=["nd_study_instance_uid"])
            final_studyuid = study.nd_study_instance_uid

            # --- Series UID ---
            if not series.nd_series_instance_uid:
                series.nd_series_instance_uid = generate_uid()
                series.save(update_fields=["nd_series_instance_uid"])
            final_seriesuid = series.nd_series_instance_uid

            # --- SOP UID ---
            if not instance.nd_sop_instance_uid:
                instance.nd_sop_instance_uid = generate_uid()
                instance.save(update_fields=["nd_sop_instance_uid"])
            final_sopuid = instance.nd_sop_instance_uid

        # Apply de-identified values to DICOM dataset
        self.dicom_ds.StudyInstanceUID = final_studyuid
        self.dicom_ds.SeriesInstanceUID = final_seriesuid
        self.dicom_ds.SOPInstanceUID = final_sopuid
        self.dicom_ds.PatientID = str(patient.nd_patient_id)

        return self.dicom_ds


class DOBDeIdentify(DicomDeidentifyRule):
    DOB_TAG = "PatientBirthDate"

    def _get_year(self, dob):
        return str(dob)[:4]
    
    def apply(self):
        dob = self.dicom_ds.get(self.DOB_TAG)
        year = self._get_year(dob)
        setattr(self.dicom_ds, self.DOB_TAG, year)
        return self.dicom_ds


class DatesDeIdentify(DicomDeidentifyRule):
    DATES_TAGS = ["StudyDate", "SeriesDate", "AcquisitionDate", "ContentDate"]
    
    def __init__(self, dicom_ds, date_offset):
        self.dicom_ds = dicom_ds
        self.date_offset = date_offset

    def apply(self):
        for tag in self.DATES_TAGS:
            date_value = self.dicom_ds.get(tag, None)
            if date_value in [None, ""]:
                continue
            new_date = apply_offset_on_date(date_value, self.date_offset)
            setattr(self.dicom_ds, tag, new_date)
        return self.dicom_ds


class MaskMetadata(DicomDeidentifyRule):
    MASK_MAPPING = [
        ("PatientName", "((PATIENTNAME))"),
        ("InstitutionName", "((FacilityName))"),
        ("InstitutionAddress", "((FacilityAddress))")
    ]

    def apply(self):
        for tag, mask_value in self.MASK_MAPPING:
            setattr(self.dicom_ds, tag, mask_value)
        return self.dicom_ds


class RemovePrivateTags(DicomDeidentifyRule):
    
    def apply(self):
        self.dicom_ds.remove_private_tags()
        return self.dicom_ds

class RemovePIITagsRule(DicomDeidentifyRule):
    PII_TAGS = [
        # Patient identifiers
        (0x0010, 0x0010),  # PatientName
        (0x0010, 0x0030),  # PatientBirthDate
        (0x0010, 0x0032),  # PatientBirthTime
        (0x0010, 0x0040),  # PatientSex
        (0x0010, 0x1000),  # OtherPatientIDs
        (0x0010, 0x1001),  # OtherPatientNames
        (0x0010, 0x2160),  # EthnicGroup
        (0x0010, 0x4000),  # PatientComments

        # Physicians / Operators
        (0x0008, 0x1070),  # OperatorsName
        (0x0008, 0x0090),  # ReferringPhysicianName
        (0x0008, 0x1050),  # PerformingPhysicianName
        (0x0008, 0x1048),  # PhysiciansOfRecord

        # Institution / Facility
        (0x0008, 0x0080),  # InstitutionName
        (0x0008, 0x0081),  # InstitutionAddress
        (0x0008, 0x1010),  # StationName
        (0x0008, 0x1040),  # InstitutionalDepartmentName
        (0x0008, 0x0082),  # InstitutionCodeSequence

        # Date-related tags
        (0x0008, 0x0021),  # SeriesDate
        (0x0008, 0x0022),  # AcquisitionDate
        (0x0008, 0x0023),  # ContentDate
        (0x0008, 0x0031),  # SeriesTime
        (0x0008, 0x0032),  # AcquisitionTime
        (0x0008, 0x0033),  # ContentTime

        (0x0008, 0x0050),  # AccessionNumber
    ]

    def apply(self):
        for tag in self.PII_TAGS:
            if tag in self.dicom_ds:
                vr = self.dicom_ds[tag].VR
                if vr in ["PN", "LO", "LT", "SH", "ST", "UT", "CS", "DA", "TM", "UI"]:
                    self.dicom_ds[tag].value = ""
                else:
                    self.dicom_ds[tag].value = 0
        return self.dicom_ds


class NDTagsRule(DicomDeidentifyRule):
    DESIRED_TAGS = {
        NdTags.DEIDENTIFICATION_DATE: ("DT", lambda: datetime.now().strftime("%Y%m%d%H%M%S")),
        NdTags.ND_PACKAGE_VERSION: ("LO", lambda: "1.0.0"),
    }

    PRIVATE_GROUP = 0x0011

    def apply(self):
        creator_tag = Tag(self.PRIVATE_GROUP, 0x0010)
        if creator_tag not in self.dicom_ds:
            self.dicom_ds.add_new(creator_tag, "LO", "NeuroDeID")

        used_elements = {tag.element for tag in self.dicom_ds.keys() if tag.group == self.PRIVATE_GROUP}

        next_element = 0x100
        for tag_name, (vr, value_fn) in self.DESIRED_TAGS.items():
            while next_element in used_elements:
                next_element += 1
                if next_element > 0xFFFE:
                    raise RuntimeError("No available private elements left in the group")

            # Add tag
            tag = Tag(self.PRIVATE_GROUP, next_element)
            self.dicom_ds.add_new(tag, vr, value_fn())
            used_elements.add(next_element)
            next_element += 1

        return self.dicom_ds
