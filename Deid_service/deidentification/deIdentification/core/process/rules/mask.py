import traceback
from typing import Any

from .helper import check_if_need_to_ignore_column
from .base import BaseDeIdentificationRule
from deIdentification.nd_logger import nd_logger
from .constants import ReusableBag
from core.dbPkg.schemas import PatientMappingDict, EncounterMappingDict


class MASKDeIdntRule(BaseDeIdentificationRule):

    @classmethod
    def de_identify_value(
        cls,
        table_name: str,
        column_config: dict,
        row: Any,
        patient_mapping_dict: dict[int, PatientMappingDict],
        encounter_mapping_dict: dict[int, EncounterMappingDict],
        re_usable_bag: ReusableBag
    ):
        column_name = column_config["column_name"]
        column_value = row[column_name]
        mask_value = column_config["mask_value"]
        try:
            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return column_value, re_usable_bag
            return f"<<{mask_value}>>", re_usable_bag
        except Exception as e:
            nd_logger.info(
                f"{'*'*80}\n"
                f"table: {table_name}, CUSTOM_STR Rule failed for the value: {column_value}\n"
                f"row: {row}\n"
                f"Failure Reason: {e}"
            )
            raise Exception(traceback.format_exc())
