import re
from .xml_deidentify import XMLDeidentifier
from .generic_pattern import GenericPatternDeIdentification
from .pii_mask import PIIValuesMasking
# from .universal_mask import UniversalPIIDeIdentifier


class UnstructuredDeidentification:
    def __init__(
        self,
    ):
        self.date_parse_cache = {}
        
    def deidentify(self, notes_text: str, pii_data: dict, insurance_data: dict, pii_config: dict, xml_config: dict):
        if (not str(notes_text)) or (len(str(notes_text)) < 3) or str(notes_text) in ["None", "null", "none", "Null", ""]:
            return notes_text
        
        offset_value = pii_data["offset_value"]
        generic_deidentifier = GenericPatternDeIdentification(notes_text, offset_value, self.date_parse_cache)
        generic_deidentifier.others()
        notes_text = generic_deidentifier.text
        pii_masker = PIIValuesMasking(pii_config=pii_config, pii_data=pii_data, insurance_data=insurance_data, text=notes_text, date_parse_cache=self.date_parse_cache)
        notes_text = pii_masker.deidentify()

        generic_deidentifier.text = notes_text
        generic_deidentifier.date()
        notes_text = generic_deidentifier.text
        
        xml_deidentifier = XMLDeidentifier(xml_config=xml_config)
        notes_text = xml_deidentifier.deidentify(notes_text)
        
        return notes_text

class GenericUnstructuredDeidentification:
    def __init__(
        self,
    ):
        self.date_parse_cache = {}
        
    def deidentify(self, notes_text: str, offset_value: int, pii_config: dict, xml_config: dict):
        if (not str(notes_text)) or (len(str(notes_text)) < 3) or str(notes_text) in ["None", "null", "none", "Null", ""]:
            return notes_text
        
        for replace_conf in pii_config.get("replace_value", []):
            old_value, new_value = replace_conf["old_value"], replace_conf["new_value"]
            pattern_id = r"(?<![\d/\-.])\b{}\b(?![\d/\-.])".format(re.escape(str(old_value)))
            notes_text = re.sub(pattern_id, str(new_value), notes_text, flags=re.IGNORECASE)
        generic_deidentifier = GenericPatternDeIdentification(notes_text, offset_value, self.date_parse_cache)
        generic_deidentifier.others()
        generic_deidentifier.date()
        notes_text = generic_deidentifier.text
        
        xml_deidentifier = XMLDeidentifier(xml_config=xml_config)
        notes_text = xml_deidentifier.deidentify(notes_text)
        
        return notes_text
