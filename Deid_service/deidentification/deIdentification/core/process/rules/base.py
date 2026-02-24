from typing import Any
import traceback

from deIdentification.nd_logger import nd_logger
from .constants import ReusableBag
from nd_api.schemas.table_config import ColumnDetailsForUI

class Rules:
    PATIENT_ID = "PATIENT_ID"
    ENCOUNTER_ID = "ENCOUNTER_ID"
    DATE_OFFSET = "DATE_OFFSET"
    MASK = "MASK"
    PATIENT_DOB = "PATIENT_DOB"
    ZIP_CODE = "ZIP_CODE"
    NOTES = "NOTES"
    GENERIC_NOTES = "GENERIC_NOTES"
    STATIC_OFFSET = "STATIC_OFFSET"
    OFFSET_32 = "32_OFFSET"
    REFER_PATIENT_ID = "REFERENCE_PID"
    REFER_PATIENT_ID_TNG = "REFER_PATIENT_ID"


class BaseDeIdentificationRule:

    @classmethod
    def de_identify_value(
        self,
        table_name: str,
        column_config: ColumnDetailsForUI,
        row: Any,
        patient_mapping_dict: dict,
        encounter_mapping_dict: dict,
        re_usable_bag: ReusableBag,
        patient_reference_mapping: dict = {}
    ) -> tuple[Any, ReusableBag]:
        return row[column_config["column_name"]], re_usable_bag


class IgnoreRowException(Exception):
    def __init__(self, message="Row should be ignored"):
        super().__init__(message)
        self.message = message

    def __str__(self):
        return f"IgnoreRowException: {self.message}"