import re
from datetime import timedelta
from dateutil import parser as date_parser
from typing import Any, Optional
import traceback
from datetime import datetime
from .constants import ReusableBag, DATE_PATTERN
from .base import BaseDeIdentificationRule
from .helper import check_if_need_to_ignore_column
from deIdentification.nd_logger import nd_logger
from core.dbPkg.schemas import PatientMappingDict, EncounterMappingDict
from django.conf import settings
from nd_api.schemas.table_config import TableDetailsForUI, ColumnDetailsForUI


def get_year(date_string):
    formats = ["%d%m%Y", "%Y%d%m", "%Y%m%d", "%m%d%Y"]
    for date_format in formats:
        try:
            return datetime.strptime(date_string, date_format).year
        except ValueError:
            continue
    if "0000" in str(date_string):
        return 0
    return int(date_parser.parse(date_string).year)


def apply_date_offset(day: str, month: str, year: str, offset_days: int):
    USELESS_VALUES = {"", "nan", "null", "none", "0", None}

    day = (
        day.strip().lower() if day and day.strip().lower() not in USELESS_VALUES else ""
    )
    month = (
        month.strip().lower()
        if month and month.strip().lower() not in USELESS_VALUES
        else ""
    )
    year = (
        year.strip().lower()
        if year and year.strip().lower() not in USELESS_VALUES
        else ""
    )

    date_str = " ".join(filter(None, [day, month, year]))
    try:
        date_obj = date_parser.parse(date_str, dayfirst=True)
    except (ValueError, date_parser.ParserError):
        return None

    adjusted_date = date_obj + timedelta(days=offset_days)

    adjusted_day = adjusted_date.day if day else None
    adjusted_month = adjusted_date.month if month else None
    adjusted_year = adjusted_date.year if year else None
    return adjusted_day, adjusted_month, adjusted_year


class DateOffsetDeIdntRule(BaseDeIdentificationRule):

    @classmethod
    def fill_offset_value(
        cls,
        patient_mapping_dict: dict[int, PatientMappingDict],
        encounter_mapping_dict: dict[int, EncounterMappingDict],
        re_usable_bag: ReusableBag,
        table_config: TableDetailsForUI,
        run_config: dict = {}
    ):
        offset_value = settings.DEFAULT_OFFSET_VALUE
        # offset_value = 0
        if re_usable_bag["patient_id"] is None:
            try:
                re_usable_bag['pid_type'] = table_config.get("enc_to_pid_column_map", run_config.get("enc_to_pid_column_map", None))
                if re_usable_bag['pid_type'] is None:
                    raise Exception(f"enc_to_pid_column_map, not defined")
                nd_logger.error(
                    f"No entry found in the mapping table for the enc-id value: {re_usable_bag['enc_id']}"
                )
                re_usable_bag["patient_id"] = int(encounter_mapping_dict[
                    re_usable_bag["enc_id"]
                ][re_usable_bag['pid_type']])
                
            except KeyError:
                pass
        try:
            offset_value = patient_mapping_dict[re_usable_bag['pid_type']][re_usable_bag["patient_id"]]["offset"]
        except KeyError:
            nd_logger.error(
                f"No entry found in the mapping table for the patient-id value: {re_usable_bag['patient_id']}"
            )
        re_usable_bag["offset_value"] = offset_value
        return re_usable_bag

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
        try:
            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return str(column_value), re_usable_bag

            if "offset_value" not in re_usable_bag:
                re_usable_bag = cls.fill_offset_value(
                    patient_mapping_dict, encounter_mapping_dict, re_usable_bag
                )

            def replace_date(match):
                original_date_str = match.group(0)
                try:
                    parsed_date = date_parser.parse(original_date_str)  
                    new_date = parsed_date + timedelta(days=re_usable_bag["offset_value"])
                    # return new_date.strftime("%Y-%m-%d")
                    return new_date.strftime("%m-%d-%Y")
                except Exception:
                    return original_date_str  # Return original if parsing fails
            date_with_offset = re.sub(
                DATE_PATTERN, replace_date, str(column_value), flags=re.IGNORECASE
            )
            nd_logger.info(
                f"deidentification done for dateoffset: {table_name} {column_value} -> {date_with_offset}"
            )
            return date_with_offset, re_usable_bag
        except Exception as e:
            nd_logger.error(
                f"{'*'*50}\n"
                f"table: {table_name}, DateOffsetDeIdntRule failed for the value: {table_name} {column_value}\n"
                f"row: {row}"
            )
            raise Exception(traceback.format_exc())

class StaticDateOffsetDeIdntRule(BaseDeIdentificationRule):

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
        try:
            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return str(column_value), re_usable_bag

            offset_value = settings.DEFAULT_OFFSET_VALUE
            
            def replace_date(match):
                original_date_str = match.group(0)
                try:
                    parsed_date = date_parser.parse(original_date_str)  
                    new_date = parsed_date + timedelta(days=offset_value)
                    return new_date.strftime("%Y-%m-%d")
                except Exception:
                    return original_date_str  # Return original if parsing fails
            date_with_offset = re.sub(
                DATE_PATTERN, replace_date, str(column_value), flags=re.IGNORECASE
            )
            nd_logger.info(
                f"deidentification done for staticdateoffset: {table_name} {column_value} -> {date_with_offset}"
            )
            return date_with_offset, re_usable_bag
        except Exception as e:
            nd_logger.error(
                f"{'*'*50}\n"
                f"table: {table_name}, StaticDateOffsetDeIdntRule failed for the value: {table_name} {column_value}\n"
                f"row: {row}"
            )
            raise Exception(traceback.format_exc())


class MergeDateOffsetDeIdntRule(BaseDeIdentificationRule):
    @classmethod
    def de_identify_value(
        self,
        table_name: str,
        column_config: dict,
        row: Any,
        patient_mapping_dict: dict[int, PatientMappingDict],
        encounter_mapping_dict: dict[int, EncounterMappingDict],
        re_usable_bag: ReusableBag
    ):
        pass

    def de_identify_value(self, source_df, column_config: dict):
        one_merge_column_info = column_config

        month_column = one_merge_column_info.get("month_column", None)
        day_column = one_merge_column_info.get("day_column", None)
        year_column = one_merge_column_info.get("year_column", None)

        rule = DateOffsetDeIdntRule(
            self.patient_id_column,
            self.enc_id_column,
            self.mapping_db,
            self.patient_to_phi_generator,
            self.table_name,
            self.progress_logs,
        )
        for idx, row in source_df.iterrows():
            offset_value = rule._get_offset_value(row)
            if offset_value is None:
                offset_value = 0
            month_value, day_value, year_value = None, None, None
            if month_column:
                month_value = row[month_column]
            if day_column:
                day_value = row[day_column]
            if year_column:
                year_value = row[year_column]
            day, month, year = apply_date_offset(
                str(day_value), str(month_value), str(year_value), offset_value
            )
            if year_column:
                source_df.loc[idx, year_column] = year
            if month_column:
                source_df.loc[idx, month_column] = month
            if day_column:
                source_df.loc[idx, day_column] = day
        return source_df


class DOBDeIdntRule(BaseDeIdentificationRule):

    @classmethod
    def de_identify_value(
        self,
        table_name: str,
        column_config: dict,
        row: Any,
        patient_mapping_dict: dict[int, PatientMappingDict],
        encounter_mapping_dict: dict[int, EncounterMappingDict],
        re_usable_bag: ReusableBag
    ):
        column_name = column_config["column_name"]
        column_value = row[column_name]
        try:
            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return str(column_value), re_usable_bag
            # self._add_entry_to_master_table(value, column_config, row)
            if column_value:
                return get_year(str(column_value)), re_usable_bag
            else:
                return None, re_usable_bag
        except Exception as e:
            nd_logger.error(
                f"{'*'*80}\n"
                f"Table: {table_name}, DOBDeIdntRule failed for the value: {column_value}\n"
                f"row: {row}\n"
                f"Failure Reason: {e}"
            )
            raise Exception(traceback.format_exc())


class ZipCodeDeIdntRule(BaseDeIdentificationRule):
    @classmethod
    def de_identify_value(
        self,
        table_name: str,
        column_config: dict,
        row: Any,
        patient_mapping_dict: dict[int, PatientMappingDict],
        encounter_mapping_dict: dict[int, EncounterMappingDict],
        re_usable_bag: ReusableBag
    ):
        try:
            column_name = column_config["column_name"]
            column_value = row[column_name]

            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return column_value, re_usable_bag
            # self._add_entry_to_master_table(value, column_config, row)
            if column_value:
                return str(column_value)[:3], re_usable_bag
            else:
                return None, re_usable_bag
        except Exception as e:
            nd_logger.error(
                f"{'*'*80}\n"
                f"table: {table_name}, ZipCodeDeIdntRule failed for the value: {column_value}\n"
                f"row: {row}\n"
                f"Failure Reason: {e}"
            )
            raise Exception(traceback.format_exc())
