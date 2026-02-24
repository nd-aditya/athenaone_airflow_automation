from .base import Detector
from .structured import (
    SDateOffestDetector,
    SDobDetector,
    SEncounterIDDetector,
    SMaskDetector,
    SPatientIdDetector,
    SZipCodeDetector,
    SStaticOffestDetector,
)
from .unstructured import UnstructuredDetector

DectorMapping = {
    "PATIENT_ID": SPatientIdDetector,
    "ENCOUNTER_ID": SEncounterIDDetector,
    "DOB": SDobDetector,
    "MASK": SMaskDetector,
    "ZIP_CODE": SZipCodeDetector,
    "DATE_OFFSET": SDateOffestDetector,
    "STATIC_OFFSET": SStaticOffestDetector,
}
