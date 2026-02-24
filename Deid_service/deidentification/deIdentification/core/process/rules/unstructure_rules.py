from datetime import datetime
from typing import Union, Optional
from django.conf import settings
from deIdentification.nd_logger import nd_logger
from .unstruct.unstruct import UnstructuredDeidentification, GenericUnstructuredDeidentification
from .decrypt import ProgressNoteDecryptor

from .constants import ReusableBag
"""
###################### Patient PII Data ######################
PatientPIIData: (Required keys)
    - patient_id

"""
class NotesDeIdntRule:
    def __init__(self, pii_data_dict: dict[int, dict], insurance_data_dict: dict[int, dict], table_config: dict, pii_config: dict, xml_config: dict):
        self.pii_data_dict = pii_data_dict
        self.insurance_data_dict = insurance_data_dict
        self.table_config = table_config
        self.pii_config = pii_config
        self.xml_config = xml_config
        # self.universal_pii_data = universal_pii_data

    def _get_pii_data(self, patient_id = None, reusable_bag: ReusableBag = {}) -> Union[Optional[dict], ReusableBag]:
        if "pii_data" in reusable_bag:
            return reusable_bag["pii_data"], reusable_bag
        if patient_id is None:
            return None, reusable_bag
        pii_data = {}
        if patient_id is not None:
            pii_data =  self.pii_data_dict.get(patient_id, {})
        return pii_data, reusable_bag
    
    def _get_insurance_data(self, patient_id = None, reusable_bag: ReusableBag = {}) -> Union[Optional[dict], ReusableBag]:
        if "insurance_data" in reusable_bag:
            return reusable_bag["insurance_data"], reusable_bag
        if patient_id is None:
            return None, reusable_bag
        insurance_data = {"metadata": {}, "values": []}
        if patient_id is not None:
            insurance_data['values'] =  self.insurance_data_dict.get("rows", {}).get(patient_id, [])
            insurance_data['metadata'] =  self.insurance_data_dict.get("metadata", {})
        return insurance_data, reusable_bag


    def de_identify_value(self, row: dict, column_value: str, column_config: dict, reusable_bag: ReusableBag):
        pii_data, reusable_bag = self._get_pii_data(reusable_bag["patient_id"], reusable_bag)
        insurance_data, reusable_bag = self._get_insurance_data(reusable_bag["patient_id"], reusable_bag)
        if pii_data is None:
            nd_logger.info(f"pii data not found for the {reusable_bag['patient_id']}, {reusable_bag['enc_id']}")
            return column_value, reusable_bag
        deidentifier = UnstructuredDeidentification()
        pii_data['nd_encounter_id'] = reusable_bag['nd_encounter_id']
        pii_data['ndid'] = reusable_bag['ndid']
        pii_data['offset_value'] = reusable_bag['offset_value']
        pii_replace_conf = []
        if "patient_id" in reusable_bag:
            pii_replace_conf.append({
                "old_value": reusable_bag["patient_id"],
                "new_value": reusable_bag["ndid"],
            })
        if "enc_id" in reusable_bag:
            pii_replace_conf.append({
                "old_value": reusable_bag["enc_id"],
                "new_value": reusable_bag["nd_encounter_id"],
            })
        temp_xml_config = self.xml_config.copy()
        replace_conf = temp_xml_config.get("replace_value", [])
        replace_conf.extend(pii_replace_conf)
        temp_xml_config["replace_value"] = replace_conf

        temp_pii_config = self.pii_config.copy()
        master_replace_conf = temp_pii_config.get("replace_value", [])
        master_replace_conf.extend(pii_replace_conf)
        temp_pii_config["replace_value"] = master_replace_conf

        if column_config.get("run_decryptor", False):
            decryptor = ProgressNoteDecryptor()
            dtmod = str(row['ModifyDate'])
            summary = str(column_value)
            column_value = decryptor.process_text(dtmod, summary)
    
        
        start_time  = datetime.now()
        notes_text = deidentifier.deidentify(str(column_value), pii_data, insurance_data, temp_pii_config, temp_xml_config)
        end_time  = datetime.now()
        
        nd_logger.info(f"Total time taken in unstruct de-identification: {end_time-start_time}")

        return notes_text, reusable_bag


class GenericNotesDeIdntRule:
    def __init__(self, pii_data_dict: dict[int, dict], insurance_data_dict: dict[int, dict], table_config: dict, pii_config: dict, xml_config: dict):
        self.pii_data_dict = pii_data_dict
        self.insurance_data_dict = insurance_data_dict
        self.table_config = table_config
        self.pii_config = pii_config
        self.xml_config = xml_config
        # self.universal_pii_data = universal_pii_data


    def de_identify_value(self, row: dict, column_value: str, column_config: dict, reusable_bag: ReusableBag):
        temp_xml_config = self.xml_config.copy()
        temp_pii_config = self.pii_config.copy()
        
        start_time  = datetime.now()
        offset_value = settings.DEFAULT_OFFSET_VALUE
        deidentifier = GenericUnstructuredDeidentification()
        notes_text = deidentifier.deidentify(str(column_value), offset_value, temp_pii_config, temp_xml_config)
        end_time  = datetime.now()
        
        nd_logger.info(f"Total time taken in unstruct de-identification: {end_time-start_time}")

        return notes_text, reusable_bag