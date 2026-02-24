from typing import TypedDict, Literal
import re
from datetime import datetime

labels_to_ignore = [
    "UK_NHS",
    "ES_NIF",
    "ES_NIE",
    "IT_FISCAL_CODE",
    "IT_DRIVER_LICENSE",
    "IT_VAT_CODE",
    "IT_PASSPORT",
    "IT_IDENTITY_CARD",
    "PL_PESEL",
    "SG_NRIC_FIN",
    "SG_UEN",
    "AU_ABN",
    "AU_ACN",
    "AU_TFN",
    "AU_MEDICARE",
    "FI_PERSONAL_IDENTITY_CODE",
    "IN_PAN",
    "IN_AADHAAR",
    "IN_VEHICLE_REGISTRATION",
    "IN_VOTER",
    "IN_PASSPORT",
]
# {
#     "dob": [
#         {
#             "tag_name": "DOB",
#             "default_replacement": "((DOB))",
#             "patterns": [
#                 "%Y-%m-%d", "%m/%d/%Y", "%Y"
#             ]
#         }
#     ],
#     "dateoffset": [
#         {
#             "tag_name": "StartDate",
#             "default_replacement": "((StartDate))",
#             "replacement_type": "pattern",
#             "patterns": [
#                 "%Y-%m-%d", "%m/%d/%Y", "%Y"
#             ]
#         }
#     ],
#     "mask": [
#         {
#             "tag_name": "GuarantorName",
#             "mask_value": "((GuarantorName))",
#         }
#     ],
#     "replace_value": [
#         {
#             "pii_key": "patient_id",
#             "old_value": 12343,
#             "new_value": 10001110101010
#         }
#     ]
# }

class XMLDeIdentificationConfig:
    dob: list[dict]
    dateoffset: list[dict]
    mask: list[dict]


class XMLDeidentifier:
    def __init__(self, xml_config: dict):
        self.xml_config = xml_config

    def _dob(self, text: str):
        def replace_dob(match, tag_name):
            date_str = match.group(1).strip()
            for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%Y"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return f"<{tag_name}>{dt.year}</{tag_name}>"
                except ValueError:
                    pass
            return f"<{tag_name}>((DOB))</{tag_name}>"
        
        for _conf in self.xml_config.get("dob", []):
            tag_name = _conf["tag_name"]
            xml_pattern = rf"<{tag_name}>(.*?)</{tag_name}>"
            text = re.sub(xml_pattern, lambda match: replace_dob(match, tag_name), text, flags=re.DOTALL)
        return text
    
    def _replace_value(self, text: str):
        for replace_conf in self.xml_config.get("replace_value", []):
            old_value, new_value = replace_conf["old_value"], replace_conf["new_value"]
            # pattern_id = r"\b{}\b".format(re.escape(str(old_value)))
            # pattern_id = r"(?<![\d/])\b{}\b(?![\d/])".format(re.escape(str(old_value)))
            # text = re.sub(pattern_id, str(new_value), text, flags=re.IGNORECASE)
        
            pattern_id = r"(?<![\d/\-.])\b{}\b(?![\d/\-.])".format(re.escape(str(old_value)))
            text = re.sub(pattern_id, str(new_value), text, flags=re.IGNORECASE)
            # pattern_id = r'(?<![\w/]){}(?![\w/])'.format(re.escape(str(old_value)))
            # text = re.sub(pattern_id, str(new_value), text, flags=re.IGNORECASE)
        return text

    def _mask(self, text: str):
        for _conf in self.xml_config.get("mask", []):
            # Build a regex that captures everything between the start_tag and end_tag
            start_tag, end_tag = f"<{_conf['tag_name']}>", f"</{_conf['tag_name']}>"
            pattern = f"{re.escape(start_tag)}(.*?){re.escape(end_tag)}"
            # Replace that content with the specified mask
            text = re.sub(pattern, f"{start_tag}{_conf['mask_value']}{end_tag}", text, flags=re.DOTALL)
        return text

    def deidentify(self, text: str) -> str:
        text = self._dob(text)
        text = self._mask(text)
        text = self._replace_value(text)
        return text



# def mask_xml_files(text, enc_patt_replace=None):
#     """
#     - Replaces occurrences of certain IDs in text based on enc_patt_replace (e.g., pat_id -> new_id).
#     - For <DOB>...</DOB>, tries to parse the date and replace with only the year. Otherwise, '((DOB))'.
#     - Masks content between certain XML tags with placeholders.

#     Args:
#         text (str): The input text containing XML-like content.
#         enc_patt_replace (dict): A dictionary mapping keys (e.g. 'pat_id') to a tuple (old_value, new_value).

#     Returns:
#         str: The masked text.
#     """

#     if enc_patt_replace is None:
#         enc_patt_replace = {"pat_id": (None, None), "enc_id": (None, None)}

#     # 1. Replace old ID values with new ones as whole words.
#     #    Example: \b11\b --> 101, \b211\b --> 11023
#     for key, (old_val, new_val) in enc_patt_replace.items():
#         pattern_id = r"\b{}\b".format(re.escape(str(old_val)))
#         text = re.sub(pattern_id, str(new_val), text)

#     # 2. Process <DOB>...</DOB> to extract or mask the year
#     #    We'll attempt to parse the content. If it fails, mask with '((DOB))'.
#     def replace_dob(match):
#         date_str = match.group(1).strip()
#         # Attempt multiple parse strategies if needed
#         for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y"):
#             try:
#                 dt = datetime.strptime(date_str, fmt)
#                 return f"<DOB>{dt.year}</DOB>"
#             except ValueError:
#                 pass
#         # If all parses fail, mask with '((DOB))'
#         return "<DOB>((DOB))</DOB>"

#     def replace_ptdob(match):
#         date_str = match.group(1).strip()
#         # Attempt multiple parse strategies if needed
#         for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y"):
#             try:
#                 dt = datetime.strptime(date_str, fmt)
#                 return f"<DOB>{dt.year}</DOB>"
#             except ValueError:
#                 pass
#         # If all parses fail, mask with '((DOB))'
#         return "<ptDob>((DOB))</ptDob>"

#     text = re.sub(r"<DOB>(.*?)</DOB>", replace_dob, text, flags=re.DOTALL)
#     text = re.sub(r"<ptDob>(.*?)</ptDob>", replace_ptdob, text, flags=re.DOTALL)

#     def replace_address(add_text):
#         pattern = re.compile(
#             r"""
#         (?P<state>(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|
#         IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|
#         NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|
#         WV|WI|WY))            # Matches the state abbreviation
#         [-\s,]*               # Matches optional dash, whitespace, or comma
#         (?P<zip>\d{5}(?:-\d{4})?) # Matches the ZIP code with optional 4-digit extension
#         """,
#             re.VERBOSE,
#         )

#         matches = pattern.findall(add_text)
#         if matches:
#             state, zip_code = matches[0]
#             return state, zip_code[:3]
#         return None, None

#     # Extract and process the <address> tag
#     address_match = re.search(r"<address>(.*?)</address>", text)
#     if address_match:
#         full_address = address_match.group(1)
#         state, zip_prefix = replace_address(full_address)
#         print(state, zip_prefix)
#         if state and zip_prefix:
#             # Replace only the <address> tag content
#             full_address_new = f"<house number> <city> {state}, {zip_prefix}"
#             text = re.sub(
#                 r"<address>.*?</address>",
#                 f"<address>{full_address_new}</address>",
#                 text,
#                 flags=re.DOTALL,
#             )

#     # 3. Mask other XML tags by replacing everything between them with placeholder text
#     mask_tags = [
#         ("<GuarantorName>", "</GuarantorName>", "((GUARANTORNAME))"),
#         ("<GuarantorId>", "</GuarantorId>", "((GUARANTORID))"),
#         ("<patient>", "</patient>", "((PATIENTNAME))"),
#         # ('<ProviderId>', '</ProviderId>', '((ProviderId))'),
#         ("<ControlNo>", "</ControlNo>", "((ControlNo))"),
#         ("<phone>", "</phone>", "((phone))"),
#         ("<StLicNo>", "</StLicNo>", "((StLicNo))"),
#         # ('<reqNo>', '</reqNo>', '((reqNo))'),
#         # ('<provider>', '</provider>', '((provider))'),
#         ("<HospitalName>", "</HospitalName>", "((HospitalName))"),
#         ("<HospitalName1>", "</HospitalName1>", "((HospitalName))"),
#         ("<HospitalName2>", "</HospitalName2>", "((HospitalName))"),
#         ("<HospitalAddress>", "</HospitalAddress>", "((HospitalAddress))"),
#         ("<HospitalAddress1>", "</HospitalAddress1>", "((HospitalAddress))"),
#         ("<HospitalAddress2>", "</HospitalAddress2>", "((HospitalAddress))"),
#         ("<HospitalAddress3>", "</HospitalAddress3>", "((HospitalAddress))"),
#         ("<HospitalPhone>", "</HospitalPhone>", "((HospitalPhone))"),
#         ("<HospitalFax>", "</HospitalFax>", "((HospitalFax))"),
#         ("<allergiesVerified>", "</allergiesVerified>", "[Allergies Verified]"),
#         ("<InvoiceId>", "</InvoiceId>", "((InvoiceId))"),
#         ("<InsuranceId>", "</InsuranceId>", "((InsuranceId))"),
#         ("<InsuranceName>", "</InsuranceName>", "((InsuranceName))"),
#         ("<PayorID>", "</PayorID>", "((PayorID))"),
#         ("<ApptFacility>", "</ApptFacility>", "((Facility Name))"),
#     ]

#     for start_tag, end_tag, mask in mask_tags:
#         # Build a regex that captures everything between the start_tag and end_tag
#         pattern = f"{re.escape(start_tag)}(.*?){re.escape(end_tag)}"
#         # Replace that content with the specified mask
#         text = re.sub(pattern, f"{start_tag}{mask}{end_tag}", text, flags=re.DOTALL)

#     return text
