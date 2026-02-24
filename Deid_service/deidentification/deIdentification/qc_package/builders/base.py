from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
from datetime import datetime, timedelta
from qc_package.schema import ColumnQCResult
from nd_api.schemas.table_config import ColumnDetailsForUI

QCErrors = {
    1: "data discrepancy present",
    2: "length verification failed",
    3: "prefix verification failed",
    4: "static offset not applied",
    5: "mask not applied",
    6: "dob rule not correctly applied",
    7: "date offset not correctly applied for some rows",
    8: "zip code rule not applied correctly for some rows",
}


class Detector(ABC):

    def __init__(
        self,
        patient_mapping_df: pd.DataFrame,
        encounter_mapping_df: pd.DataFrame,
        appointment_mapping_df: pd.DataFrame,
        qc_config: dict,
        column_config: ColumnDetailsForUI,
        patient_id_cols: str,
        encounter_id_cols: str,
        appointment_id_cols: str,
    ):
        self.patient_mapping_df = patient_mapping_df
        self.encounter_mapping_df = encounter_mapping_df
        self.appointment_mapping_df = appointment_mapping_df
        self.qc_config = qc_config
        self.column_config = column_config
        self.column_name = self.column_config["column_name"]

        self.patient_id_cols = patient_id_cols
        self.encounter_id_cols = encounter_id_cols
        self.appointment_id_cols = appointment_id_cols

    @abstractmethod
    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        pass
