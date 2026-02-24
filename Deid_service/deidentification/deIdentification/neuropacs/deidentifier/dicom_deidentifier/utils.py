class NdTags:
    DEIDENTIFICATION_DATE = "DEIDENTIFICATION_DATE"
    ND_PACKAGE_VERSION = "ND_PACKAGE_VERSION"


def apply_offset_on_date(date_value: str, date_offset: int):
    return date_value