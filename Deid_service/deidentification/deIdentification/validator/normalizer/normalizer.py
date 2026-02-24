import re
# from medical_normalizer.normalizer import MedicalNormalizer  # from our earlier design

# medical_normalizer/normalizer.py
import requests
from typing import Dict, Any
from validator.normalizer.mapping import snomed_mappers, icd10_mapper, rxnorm_mapper, loinc_mapper
# from mapping.snomed_mapper import lookup as sn_lookup
from validator.normalizer.utils import clean_text

class MedicalNormalizer:
    def __init__(self):
        pass

    def normalize(self, text: str) -> Dict[str, Any]:
        """
        Normalize free-text clinical term to standard terminologies.
        """
        cleaned = clean_text(text)
        result = {}

        # Try SNOMED CT
        snomed_code = snomed_mappers.lookup(cleaned)
        if snomed_code:
            result["SNOMED_CT"] = snomed_code

        # Try ICD-10
        icd10_code = icd10_mapper.lookup(cleaned)
        if icd10_code:
            result["ICD10"] = icd10_code

        # Try RxNorm
        rx_code = rxnorm_mapper.lookup(cleaned)
        if rx_code:
            result["RxNorm"] = rx_code

        # Try LOINC
        loinc_code = loinc_mapper.lookup(cleaned)
        if loinc_code:
            result["LOINC"] = loinc_code

        return result


normalizer = MedicalNormalizer()

def standardize_text(text: str) -> str:
    # Extract candidate terms - in real case, use NLP (MedSpaCy / QuickUMLS)
    terms = re.findall(r"[A-Za-z][A-Za-z ]{2,}", text)
    # breakpoint()

    for term in terms:
        mapping = normalizer.normalize(term)
        # If mapping found, replace in text with standard form
        if mapping.get("SNOMED_CT"):
            code = mapping["SNOMED_CT"]["code"]
            display = mapping["SNOMED_CT"]["display"]
            text = re.sub(rf"\b{term}\b", f"{display} [SNOMED:{code}]", text, flags=re.IGNORECASE)
        elif mapping.get("ICD10"):
            code = mapping["ICD10"]["code"]
            display = mapping["ICD10"]["display"]
            text = re.sub(rf"\b{term}\b", f"{display} [ICD10:{code}]", text, flags=re.IGNORECASE)

    return text
