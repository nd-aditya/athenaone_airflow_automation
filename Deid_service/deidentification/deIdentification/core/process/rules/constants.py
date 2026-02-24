from typing import TypedDict
import operator

# DATE_PATTERN = (
#     r"(?:0?[1-9]|1[0-2])[/-]\d{1,2}[/-]\d{4}(?!\d)|"  # MM/DD/YYYY or M/D/YYYY
#     r"(?:0?[1-9]|1[0-2])[/-]\d{1,2}[/-]\d{2}(?!\d)|"  # MM/DD/YY or M/D/YY
#     r"\d{1,2}\s(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{4}(?!\d)|"  # DD Month YYYY
#     r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{1,2}(?:st|nd|rd|th)?[,/\s-]\s?\d{4}(?!\d)|"  # Month DD, YYYY with ordinals
#     r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{1,2},?\s\d{4}(?!\d)|"  # Full month names with DD, YYYY
#     r"(?:0?[1-9]|1[0-2])[-.]\d{1,2}[-.]\d{2}(?!\d)|"  # MM-DD-YY or M-D-YY
#     r"\d{4}-(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12]\d|3[01])|"  # YYYY-MM-DD
#     r"(?:0?[1-9]|[12]\d|3[01])[-/](?:0?[1-9]|1[0-2])[-/]\d{4}(?!\d)|"  # DD/MM/YYYY or DD-MM-YYYY
#     r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)|Nov(?:ember)|Dec(?:ember)?)\.\s\d{1,2}\.\d{4}(?!\d)|"  # Mon. DD. YYYY
#     r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?!\d)|"  # YYYY-MM-DD HH:mm:SS
#     r"\d{4}\d{2}\d{2}(?!\d)|"  # YYYYMMDD
#     r"(?:0?[1-9]|[12]\d|3[01])(?:0?[1-9]|1[0-2])\d{4}(?!\d)|"  # DDMMYYYY
#     r"(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{4}(?!\d)|"  # MMDDYYYY
#     r"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}|(?:0?[1-9]|1[0-2])-\d{4}-\d{2}\s\d{2}:\d{2}:\d{2}|"  # YYYY-MM-DD HH:mm:SS, MM-YYYY-DD HH:mm:SS
#     r"(?:0?[1-9]|1[0-2])-\d{2}-\d{4}\s\d{2}:\d{2}:\d{2}"  # MM-DD-YYYY HH:mm:SS
# )
DATE_PATTERN = (
    r"\b(?:0?[1-9]|1[0-2])[/-]\d{1,2}[/-]\d{4}(?!\d)\b|"  # MM/DD/YYYY or M/D/YYYY
    r"\b(?:0?[1-9]|1[0-2])[/-]\d{1,2}[/-]\d{2}(?!\d)\b|"  # MM/DD/YY or M/D/YY
    r"\b\d{1,2}\s(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{4}(?!\d)\b|"  # DD Month YYYY
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s\d{1,2}(?:st|nd|rd|th)?[,/\s-]\s?\d{4}(?!\d)\b|"  # Month DD, YYYY (with ordinals)
    r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{1,2},?\s\d{4}(?!\d)\b|"  # Full month names with DD, YYYY
    r"\b(?:0?[1-9]|1[0-2])[-.]\d{1,2}[-.]\d{2}(?!\d)\b|"  # MM-DD-YY or M-D-YY
    r"\b\d{4}-(?:0?[1-9]|1[0-2])-(?:0?[1-9]|[12]\d|3[01])\b|"  # YYYY-MM-DD
    r"\b(?:0?[1-9]|[12]\d|3[01])[-/](?:0?[1-9]|1[0-2])[-/]\d{4}(?!\d)\b|"  # DD/MM/YYYY or DD-MM-YYYY
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)|Nov(?:ember)|Dec(?:ember)?)\.\s\d{1,2}\.\d{4}(?!\d)\b|"  # Mon. DD. YYYY
    r"\b\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?!\d)\b|"  # YYYY-MM-DD HH:mm:SS
    r"\b\d{4}\d{2}\d{2}(?!\d)\b|"  # YYYYMMDD
    r"\b(?:0?[1-9]|[12]\d|3[01])(?:0?[1-9]|1[0-2])\d{4}(?!\d)\b|"  # DDMMYYYY
    r"\b(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{4}(?!\d)\b|"  # MMDDYYYY
    r"\b\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\b|"  # YYYY-MM-DD HH:mm:SS
    r"\b(?:0?[1-9]|1[0-2])-\d{4}-\d{2}\s\d{2}:\d{2}:\d{2}\b|"  # MM-YYYY-DD HH:mm:SS
    r"\b(?:0?[1-9]|1[0-2])-\d{2}-\d{4}\s\d{2}:\d{2}:\d{2}\b"  # MM-DD-YYYY HH:mm:SS
)


class OPERATORS:
    AND = "and"
    OR = "or"

class Conditions:
    LESS_THAN = "lt"
    GREATER_THAN = "gt"
    EQUAL = "eq"
    NOT_EQUAL = "neq"
    IN = "in"
    NOT_IN = "not_in"

ConditionToOperatorMapping = {
    Conditions.LESS_THAN: operator.lt,
    Conditions.GREATER_THAN: operator.gt,
    Conditions.EQUAL: operator.eq,
    Conditions.NOT_EQUAL: operator.ne,
    Conditions.IN: lambda x, y: x in y,
    Conditions.NOT_IN: lambda x, y: x not in y,
}

class ReusableBag(TypedDict, total=False):
    # it will be at row level and not for whole table
    patient_id: int
    pid_type: str
    nd_id: int
    enc_id: int
    nd_encounter_id: int
    offset_value: int
    pii_data: dict
    insurance_data: list[dict]
