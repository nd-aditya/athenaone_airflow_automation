from sqlalchemy import (
    Column,
    Integer,
    String,
)

TYPE_MAPPING = {
    "Integer": Integer,
    "String": String,
}

def parse_patientid_column_string(column_str: str):
    columns = []
    for item in column_str.split("),"):
        item = item.strip(" ()")
        parts = [x.strip() for x in item.split(",")]
        name, type_str = parts[0], parts[1]
        kwargs = {}
        if len(parts) > 2:
            # Parse additional kwargs like primary_key=True
            for extra in parts[2:]:
                key, val = extra.split("=")
                kwargs[key.strip()] = eval(val.strip())  # Safe only if input is trusted
        columns.append((name, Column(TYPE_MAPPING[type_str], **kwargs)))
    return columns
