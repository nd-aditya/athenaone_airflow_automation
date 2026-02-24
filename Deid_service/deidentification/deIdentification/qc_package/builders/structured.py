import pandas as pd
from datetime import datetime
from qc_package.builders.base import Detector
from qc_package.schema import ColumnQCResult
from django.conf import settings
from deIdentification.nd_logger import nd_logger


class SZipCodeDetector(Detector):
    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        nd_logger.info(f"SZipCodeDetector: Starting deidentification check for column {self.column_name}")
        col_series = after_df[self.column_name].astype(str).str.lower()
        passed_mask = (col_series.str.len() <= 3) | col_series.isin(["none", "null"])
        passed_count = passed_mask.sum()
        failed_count = len(after_df) - passed_count
        nd_logger.info(f"SZipCodeDetector: Passed={passed_count}, Failed={failed_count}")
        failure_reasons = []
        if failed_count > 0:
            nd_logger.warning(f"SZipCodeDetector: Failed rows present. Appending failure code 8.")
            failure_reasons.append(8)
        failure_nd_auto_incr_ids = after_df.loc[
            ~passed_mask, "nd_auto_increment_id"
        ].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SZipCodeDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )


class SDobDetector(Detector):
    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        nd_logger.info(f"SDobDetector: Starting deidentification check for column {self.column_name}")
        col_series = after_df[self.column_name].astype(str).str.lower()
        valid_year = col_series.str.isdigit() & (col_series.str.len() == 4)
        empty_mask = col_series.isin(["", "none", "null"])
        passed_mask = valid_year | empty_mask
        passed_count = passed_mask.sum()
        failed_count = len(after_df) - passed_count
        nd_logger.info(f"SDobDetector: Passed={passed_count}, Failed={failed_count}")
        failure_reasons = []
        if failed_count > 0:
            nd_logger.warning(f"SDobDetector: Failed rows present. Appending failure code 6.")
            failure_reasons.append(6)
        failure_nd_auto_incr_ids = after_df.loc[
            ~passed_mask, "nd_auto_increment_id"
        ].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SDobDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )


class SStaticOffestDetector(Detector):
    def get_offset(self, row):
        return settings.DEFAULT_OFFSET_VALUE

    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        nd_logger.info(f"SStaticOffestDetector: Starting offset deidentification check for column {self.column_config['column_name']}")
        column_name = self.column_config["column_name"]
        merged_df = after_df.merge(
            before_df[[column_name, "nd_auto_increment_id"]],
            on="nd_auto_increment_id",
            suffixes=("_after", "_before"),
        )

        merged_df["date_before"] = pd.to_datetime(
            merged_df[f"{column_name}_before"], errors="coerce"
        )
        merged_df["date_after"] = pd.to_datetime(
            merged_df[f"{column_name}_after"], errors="coerce"
        )
        merged_df["date_before"] = pd.to_datetime(merged_df["date_before"], errors="coerce")
        merged_df["date_after"] = pd.to_datetime(merged_df["date_after"], errors="coerce")

        merged_df["date_diff"] = (
            merged_df["date_after"].dt.normalize() - merged_df["date_before"].dt.normalize()
        ).dt.days
        merged_df["passed"] = merged_df["date_diff"] == settings.DEFAULT_OFFSET_VALUE
        
        passed_count = merged_df["passed"].sum()
        failed_df = merged_df[~merged_df["passed"]]
        failed_count = len(failed_df)

        nd_logger.info(f"SStaticOffestDetector: Passed={passed_count}, Failed={failed_count}")

        failure_reasons = []
        if failed_count > 0:
            nd_logger.warning(f"SStaticOffestDetector: Failed rows present. Appending failure code 4.")
            failure_reasons.append(4)
        failure_nd_auto_incr_ids = failed_df["nd_auto_increment_id"].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SStaticOffestDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )


class SMaskDetector(Detector):
    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        mask_value = self.column_config["mask_value"]
        nd_logger.info(f"SMaskDetector: Starting mask deidentification check for column {self.column_name} using mask value '<<{mask_value}>>'")
        col_series = after_df[self.column_name].astype(str)
        passed_mask = col_series == f"<<{mask_value}>>"
        passed_count = passed_mask.sum()
        failed_count = len(after_df) - passed_count
        nd_logger.info(f"SMaskDetector: Passed={passed_count}, Failed={failed_count}")
        failure_reasons = []
        if failed_count > 0:
            nd_logger.warning(f"SMaskDetector: Failed rows present. Appending failure code 5.")
            failure_reasons.append(5)
        failure_nd_auto_incr_ids = after_df.loc[
            ~passed_mask, "nd_auto_increment_id"
        ].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SMaskDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )

class SDateOffestDetector(Detector):
    def get_offset(self, row):
        try:
            if len(self.patient_id_cols)>0:
                ndpid = row[self.patient_id_cols[0]]
                filtered = self.patient_mapping_df[self.patient_mapping_df['nd_patient_id'] == ndpid]
                if not filtered.empty:
                    nd_logger.debug(f"SDateOffestDetector: Offset fetched by patient_id: {filtered['offset'].iloc[0]}")
                    return int(filtered['offset'].iloc[0])
            if len(self.encounter_id_cols)>0:
                nd_enc_id = row[self.encounter_id_cols[0]]
                filtered = self.encounter_mapping_df[self.encounter_mapping_df['nd_encounter_id'] == nd_enc_id]
                if not filtered.empty:
                    ndpid = int(filtered['nd_patient_id'].iloc[0])
                    pt_filtered = self.patient_mapping_df[self.patient_mapping_df['nd_patient_id'] == ndpid]
                    nd_logger.debug(f"SDateOffestDetector: Offset fetched by encounter_id -> patient_id: {pt_filtered['offset'].iloc[0]}")
                    return int(pt_filtered["offset"].iloc[0])
            if len(self.appointment_id_cols)>0:
                appt_id = row[self.appointment_id_cols[0]]
                filtered = self.encounter_mapping_df[self.encounter_mapping_df['nd_appointment_id'] == appt_id]
                if not filtered.empty:
                    ndpid = int(filtered['nd_patient_id'].iloc[0])
                    pt_filtered = self.patient_mapping_df[self.patient_mapping_df['nd_patient_id'] == ndpid]
                    nd_logger.debug(f"SDateOffestDetector: Offset fetched by appointment_id -> patient_id: {pt_filtered['offset'].iloc[0]}")
                    return int(pt_filtered["offset"].iloc[0])
        except Exception as e:
            nd_logger.error(f"SDateOffestDetector: Error determining offset: {e}")
        nd_logger.debug("SDateOffestDetector: Using default offset value")
        return settings.DEFAULT_OFFSET_VALUE

    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        nd_logger.info(f"SDateOffestDetector: Starting offset deidentification check for column {self.column_config['column_name']}")
        column_name = self.column_config["column_name"]
        merged_df = after_df.merge(
            before_df[[column_name, "nd_auto_increment_id"]],
            on="nd_auto_increment_id",
            suffixes=("_after", "_before"),
        )

        merged_df["date_before"] = pd.to_datetime(
            merged_df[f"{column_name}_before"], errors="coerce"
        )
        merged_df["date_after"] = pd.to_datetime(
            merged_df[f"{column_name}_after"], errors="coerce"
        )
        merged_df["offset"] = merged_df.apply(self.get_offset, axis=1)
        merged_df["date_before"] = pd.to_datetime(merged_df["date_before"], errors="coerce")
        merged_df["date_after"] = pd.to_datetime(merged_df["date_after"], errors="coerce")

        merged_df["date_diff"] = (
            merged_df["date_after"].dt.normalize() - merged_df["date_before"].dt.normalize()
        ).dt.days

        merged_df["passed"] = merged_df["date_diff"] == merged_df["offset"]

        passed_count = merged_df["passed"].sum()
        failed_df = merged_df[~merged_df["passed"]]
        failed_count = len(failed_df)

        nd_logger.info(f"SDateOffestDetector: Passed={passed_count}, Failed={failed_count}")

        failure_reasons = []
        if failed_count > 0:
            nd_logger.warning("SDateOffestDetector: Failed rows present. Appending failure code 7.")
            failure_reasons.append(7)
        failure_nd_auto_incr_ids = failed_df["nd_auto_increment_id"].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SDateOffestDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )


class SPatientIdDetector(Detector):
    def _verify_length(self, series: pd.Series) -> pd.Series:
        length = self.qc_config.get("PATIENT_ID", {}).get("length_of_value")
        return (
            series.astype(str).str.len() == length
            if length
            else pd.Series([True] * len(series))
        )

    def _verify_prefix(self, series: pd.Series) -> pd.Series:
        prefix = self.qc_config.get("PATIENT_ID", {}).get("prefix_value")
        return (
            series.astype(str).str.startswith(prefix)
            if prefix
            else pd.Series([True] * len(series))
        )

    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        nd_logger.info(f"SPatientIdDetector: Starting ID check for column {self.column_name}")
        col_series = after_df[self.column_name]
        length_check = self._verify_length(col_series)
        prefix_check = self._verify_prefix(col_series)
        passed_mask = length_check & prefix_check
        passed_count = passed_mask.sum()
        failed_count = len(after_df) - passed_count
        nd_logger.info(f"SPatientIdDetector: Passed={passed_count}, Failed={failed_count}")
        failure_reasons = []
        length_failed = (~length_check).sum()
        prefix_failed = (~prefix_check).sum()
        if length_failed > 0:
            nd_logger.warning(f"SPatientIdDetector: Failed length check for {length_failed} rows, appending failure code 2.")
            failure_reasons.append(2)
        if prefix_failed > 0:
            nd_logger.warning(f"SPatientIdDetector: Failed prefix check for {prefix_failed} rows, appending failure code 3.")
            failure_reasons.append(3)
        failure_nd_auto_incr_ids = after_df.loc[
            ~passed_mask, "nd_auto_increment_id"
        ].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SPatientIdDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )


class SEncounterIDDetector(Detector):
    def _verify_length(self, series: pd.Series) -> pd.Series:
        length = self.qc_config.get("ENCOUNTER_ID", {}).get("length_of_value")
        return (
            series.astype(str).str.len() == length
            if length
            else pd.Series([True] * len(series))
        )

    def _verify_prefix(self, series: pd.Series) -> pd.Series:
        prefix = self.qc_config.get("ENCOUNTER_ID", {}).get("prefix_value")
        return (
            series.astype(str).str.startswith(prefix)
            if prefix
            else pd.Series([True] * len(series))
        )

    def is_deidentified(
        self, before_df: pd.DataFrame, after_df: pd.DataFrame
    ) -> ColumnQCResult:
        nd_logger.info(f"SEncounterIDDetector: Starting ID check for column {self.column_name}")
        col_series = after_df[self.column_name]
        length_check = self._verify_length(col_series)
        prefix_check = self._verify_prefix(col_series)
        passed_mask = length_check & prefix_check
        passed_count = passed_mask.sum()
        failed_count = len(after_df) - passed_count
        nd_logger.info(f"SEncounterIDDetector: Passed={passed_count}, Failed={failed_count}")
        failure_reasons = []
        length_failed = (~length_check).sum()
        prefix_failed = (~prefix_check).sum()
        if length_failed > 0:
            nd_logger.warning(f"SEncounterIDDetector: Failed length check for {length_failed} rows, appending failure code 2.")
            failure_reasons.append(2)
        if prefix_failed > 0:
            nd_logger.warning(f"SEncounterIDDetector: Failed prefix check for {prefix_failed} rows, appending failure code 3.")
            failure_reasons.append(3)
        failure_nd_auto_incr_ids = after_df.loc[
            ~passed_mask, "nd_auto_increment_id"
        ].tolist()
        if failure_nd_auto_incr_ids:
            nd_logger.debug(f"SEncounterIDDetector: Failing nd_auto_increment_ids: {failure_nd_auto_incr_ids}")
        return ColumnQCResult(
            passed_count=passed_count,
            failed_count=failed_count,
            failure_reasons=failure_reasons,
            failure_nd_auto_incr_ids=failure_nd_auto_incr_ids,
            remarks={}
        )
