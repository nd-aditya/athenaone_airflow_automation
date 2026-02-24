from typing import Any
import traceback

from .constants import ReusableBag
from .base import BaseDeIdentificationRule, IgnoreRowException
from .helper import check_if_need_to_ignore_column
from deIdentification.nd_logger import nd_logger
from core.dbPkg.schemas import PatientMappingDict, EncounterMappingDict
from core.ops.jointables import ReferenceMapping
from nd_api.schemas.table_config import ColumnDetailsForUI


class PatientIDDeIdntRule(BaseDeIdentificationRule):

    @classmethod
    def de_identify_value(
        self,
        table_name: str,
        column_config: ColumnDetailsForUI,
        row: Any,
        patient_mapping_dict: dict[str, dict[int, PatientMappingDict]],
        encounter_mapping_dict: dict[int, EncounterMappingDict],
        re_usable_bag: ReusableBag
    ):
        column_name = column_config["column_name"]
        column_value = row[column_name]
        pid_type = column_config['de_identification_rule'].replace("PATIENT_ID_", "")
        try:
            int_patient_id = int(column_value)
            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return column_value, re_usable_bag
            nd_patient_id = patient_mapping_dict.get(pid_type, {}).get(int_patient_id, {}).get(
                "nd_patient_id", int_patient_id
            )
            return str(nd_patient_id), re_usable_bag
        except (TypeError, OverflowError) as e:
            nd_logger.error(f"not able to convert patient-id to int {column_value}, {type(column_value)}, \n {e}")
            return None, re_usable_bag
        except ValueError as e:
            nd_logger.error(f"value error for patinet-id {column_value}, {type(column_value)}, \n {e}")
            raise IgnoreRowException(e)
        except Exception as e:
            nd_logger.error(
                f"{'*'*80}\n"
                f"table: {table_name}, PatientIDDeIdntRule failed for the value: {column_value}\n"
                f"row: {row}"
            )
            raise Exception(traceback.format_exc())
# class PatientIDDeIdntRule(BaseDeIdentificationRule):

#     @classmethod
#     def de_identify_value(
#         self,
#         table_name: str,
#         column_config: dict,
#         row: Any,
#         patient_mapping_dict: dict[int, PatientMappingDict],
#         encounter_mapping_dict: dict[int, EncounterMappingDict],
#         re_usable_bag: ReusableBag
#     ):
#         column_name = column_config["column_name"]
#         column_value = row[column_name]
#         try:
#             int_patient_id = int(column_value)
#             ignore = check_if_need_to_ignore_column(column_config, row)
#             if ignore:
#                 return column_value, re_usable_bag
#             nd_patient_id = patient_mapping_dict.get(int_patient_id, {}).get(
#                 "nd_patient_id", int_patient_id
#             )
#             return str(nd_patient_id), re_usable_bag
#         except (TypeError, OverflowError) as e:
#             nd_logger.error(f"not able to convert patient-id to int {column_value}, {type(column_value)}, \n {e}")
#             return None, re_usable_bag
#         except ValueError as e:
#             nd_logger.error(f"value error for patinet-id {column_value}, {type(column_value)}, \n {e}")
#             raise IgnoreRowException(e)
#         except Exception as e:
#             nd_logger.error(
#                 f"{'*'*80}\n"
#                 f"table: {table_name}, PatientIDDeIdntRule failed for the value: {column_value}\n"
#                 f"row: {row}"
#             )
#             raise Exception(traceback.format_exc())

# class REFERPatientIDDeIdntRule(BaseDeIdentificationRule):

#     @classmethod
#     def de_identify_value(
#         self,
#         table_name: str,
#         column_config: dict,
#         row: Any,
#         patient_mapping_dict: dict[int, PatientMappingDict],
#         encounter_mapping_dict: dict[int, EncounterMappingDict],
#         re_usable_bag: ReusableBag,
#         patient_reference_mapping: ReferenceMapping = {}
#     ):
#         column_name = column_config["column_name"]
#         column_value = row[column_name]
#         try:
#             refer_pid = int(float(str(column_value)))
#         except:
#             nd_logger.error(f"not able to convet value to int refer-patinet-id {column_value}, {type(column_value)}, {e}")
#             return None, re_usable_bag
#         try:
#             ignore = check_if_need_to_ignore_column(column_config, row)
#             if ignore:
#                 return column_value, re_usable_bag
#             patient_id = patient_reference_mapping["reference_mapping"][refer_pid]
#             nd_patient_id = patient_mapping_dict[patient_id].get('nd_patient_id', None)
#             nd_logger.info(f"refer-patient-id: patient_id {patient_id},  for refer-id {refer_pid}, nd-patient-id: {nd_patient_id}")
            
#             return nd_patient_id, re_usable_bag
#         except (TypeError) as e:
#             nd_logger.error(f"not able to convert refer-pid-id to int {column_value}, {type(column_value)}, \n {e}, TypeError")
#             return None, re_usable_bag
#         except (OverflowError) as e:
#             nd_logger.error(f"not able to convert refer-pid-id to int {column_value}, {type(column_value)}, \n {e}, OverflowError")
#             return None, re_usable_bag
#         except (ValueError) as e:
#             nd_logger.error(f"value error for refer-patinet-id {column_value}, {type(column_value)}, {e}")
#             raise IgnoreRowException(e)
#         except (KeyError) as e:
#             nd_logger.error(f"keys error for refer-patinet-id {column_value}, {type(column_value)}, {e}")
#             raise IgnoreRowException(e)
#         except Exception as e:
#             nd_logger.error(
#                 f"{'*'*80}\n"
#                 f"table: {table_name}, PatientIDDeIdntRule failed for the value: {column_value}\n"
#                 f"row: {row}"
#             )
#             raise Exception(traceback.format_exc())


class EncounterIDDeIdntRule(BaseDeIdentificationRule):

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
            int_enc_id = int(column_value)
            ignore = check_if_need_to_ignore_column(column_config, row)
            if ignore:
                return int_enc_id, re_usable_bag
            nd_enc_id = encounter_mapping_dict.get(int_enc_id, {}).get(
                "nd_encounter_id", int_enc_id
            )
            return str(nd_enc_id), re_usable_bag
        except TypeError as e:
            nd_logger.error(f"not able to convert patient-id to int {column_value}, {type(column_value)}, \n {e}")
            return column_value, re_usable_bag
        except ValueError as e:
            nd_logger.error(f"value error for enc-id {column_value}, {type(column_value)}, \n {e}")
            raise IgnoreRowException(e)
        except Exception as e:
            nd_logger.error(
                f"{'*'*80}\n"
                f"table {table_name}, EncounterIDDeIdntRule failed for the value: {column_value}\n"
                f"row: {row}"
            )
            raise Exception(traceback.format_exc())
