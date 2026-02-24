import re
import itertools
from .date_parse import cached_parse_date
from .utils import GENERIC_DATE_REGEX


def reformat_date(match):
    # Comprehensive date reformation covering multiple group patterns
    groups = {
        'day': ['day1', 'day2', 'day3', 'day4', 'day5', 'day6', 'day7', 'day8', 'day9'],
        'month': ['month1', 'month2', 'month3', 'month4', 'month5', 'month6', 'month7', 'month8', 'month9', 'month10', 'month11', 'month12'],
        'year': ['year1', 'year2', 'year3', 'year4', 'year5', 'year6', 'year7', 'year8', 'year9', 'year10']
    }

    month_mapping = {
        'January': '01', 'Jan': '01', 
        'February': '02', 'Feb': '02',
        'March': '03', 'Mar': '03',
        'April': '04', 'Apr': '04',
        'May': '05',
        'June': '06', 'Jun': '06',
        'July': '07', 'Jul': '07',
        'August': '08', 'Aug': '08',
        'September': '09', 'Sep': '09', 'Sept': '09',
        'October': '10', 'Oct': '10',
        'November': '11', 'Nov': '11',
        'December': '12', 'Dec': '12'
    }

    # Find first non-None value for each group type
    day = next((match.group(g) for g in groups['day'] if match.group(g)), None)
    month = next((match.group(g) for g in groups['month'] if match.group(g)), None)
    year = next((match.group(g) for g in groups['year'] if match.group(g)), None)

    # Standardize month to two-digit format
    if month and month in month_mapping:
        month = month_mapping[month]

    # Normalize year to 4 digits
    if year and len(year) == 2:
        year = '20' + year if int(year) <= 50 else '19' + year

    # Return formatted date based on available components
    if day and month and year:
        # return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
    elif month and year:
        # return f"{year}-{month.zfill(2)}"
        return f"{month.zfill(2)}/{year}"
    
    return "UNKNOWN_DATE"

def replace_dates(text, replacements):
    def replacement_function(match):
        formatted_date = reformat_date(match)
        return replacements.get(formatted_date, match.group(0))
    
    # Compile the date pattern from the GENERIC_REGEX_DICT
    date_pattern = re.compile(GENERIC_DATE_REGEX["regex"], re.VERBOSE | re.IGNORECASE)
    
    # Use the compiled pattern to substitute dates in the text
    return date_pattern.sub(replacement_function, text)

def formate_date(text):
    def replacement_function(match):
        formatted_date = reformat_date(match)
        return formatted_date
        
    
    # Compile the date pattern from the GENERIC_REGEX_DICT
    date_pattern = re.compile(GENERIC_DATE_REGEX["regex"], re.VERBOSE | re.IGNORECASE)
    
    # Use the compiled pattern to substitute dates in the text
    return date_pattern.sub(replacement_function, text)


class PIIValuesMasking:
    def __init__(self, pii_config: dict, pii_data: dict, insurance_data: list[dict], text: str, date_parse_cache: dict):
        self.pii_config = pii_config
        self.pii_data = pii_data
        self.insurance_data = insurance_data
        self.text = text
        self.date_parse_cache = date_parse_cache

    def mask_insurance_data(self):
        metadata = self.insurance_data.get("metadata", {})
        for row in self.insurance_data.get("values", []):
            for col, value in row.items():
                processed_value = str(value).strip()
                if processed_value and processed_value.lower() not in {"none", "null"}:
                    column_conf = metadata.get(col, {})
                    mask_value = column_conf.get("mask_value", f"(({col}))")
                    min_length = column_conf.get("min_length", None)
                    if min_length is not None and len(processed_value) < min_length:
                        continue
                    processed_value = f" {processed_value} "
                    self.text = self.text.replace(processed_value, f" {mask_value} ")

    def deidentify(self):
        for key, conf in self.pii_config.get("regex", {}).items():
            patterns = (
                conf["regex"]
                if isinstance(conf["regex"], list)
                else [conf["regex"]]
            )
            for pat in patterns:
                matches = re.compile(pat, re.IGNORECASE)
                self.text = matches.sub(conf["masking_value"], self.text)
        
        date_maps = {}
        name_map = {}
        # direct_map will map the original string (case-insensitive) to the replacement
        for pii_name, conf in self.pii_config.get("combine", {}).items():
            mask_val = conf["masking_value"]
            full_name_lst = []
            name_parts = [
                str(self.pii_data[i])
                for i in conf["combine"]
                if self.pii_data.get(i) and len(self.pii_data.get(i)) > 2
            ]
            for r in range(1, len(name_parts) + 1):
                for combo in itertools.product(name_parts, repeat=r):
                    combined_name = "".join(combo)  # Combine parts without space
                    full_name_lst.append(combined_name)

            # Add each combination to the name_map with the appropriate masking value
            for i in full_name_lst:
                name_map[i.lower()] = mask_val
        for key, value in self.pii_config.get("mask", {}).items():
            mask_val = value["masking_value"]
            original_str = str(self.pii_data[key]).strip()
            if len(original_str)< 1:
                continue
            if original_str and len(str(original_str)) > 2:
                # original_str = str(pii_data[key])
                mask_val = value["masking_value"]
                original_str = f"{original_str.strip()}" if original_str else ""
                name_map[original_str.lower()] = mask_val
        
        for key, conf in self.pii_config.get("dob", {}).items():
            mask_val = conf["masking_value"]
            original_str = str(self.pii_data[key])
            original_str = replace_dates(original_str, {})
            (new_date, new_year), self.date_parse_cache = cached_parse_date(self.date_parse_cache, original_str, offset=0)
            replacement = str(new_year) if new_year else mask_val
            date_maps[original_str.lower()] = replacement

        if name_map:
            for name, masked_value in name_map.items():
                # Create a regex pattern that matches the name as a whole word
                pattern = r"\b" + re.escape(name) + r"\b"
                # Replace occurrences of the name in the text with the masked value
                self.text = re.sub(
                    pattern, masked_value, self.text, flags=re.IGNORECASE
                )

        if date_maps:
            # Sort by length to replace longer matches first (optional optimization)
            sorted_keys = sorted(date_maps.keys(), key=len, reverse=True)
            pattern = re.compile(
                "|".join(re.escape(k) for k in sorted_keys), re.IGNORECASE
            )

            def direct_repl_func(m):
                matched_lower = m.group(0).lower()
                return date_maps[matched_lower]

            self.text = pattern.sub(direct_repl_func, self.text)
        self.text = replace_dates(self.text, date_maps)

        for replace_conf in self.pii_config.get("replace_value", []):
            old_value, new_value = replace_conf["old_value"], replace_conf["new_value"]
            # pattern_id = r"\b{}\b".format(re.escape(str(old_value)))
            # pattern_id = r"(?<![\d/])\b{}\b(?![\d/])".format(re.escape(str(old_value)))
            # self.text = re.sub(pattern_id, str(new_value), self.text, flags=re.IGNORECASE)

            pattern_id = r"(?<![\d/\-.])\b{}\b(?![\d/\-.])".format(re.escape(str(old_value)))
            self.text = re.sub(pattern_id, str(new_value), self.text, flags=re.IGNORECASE)
            
        
        self.mask_insurance_data()
        return self.text

################## EXAMPLE PII_CONFIG ##################
master_pii_dict = {
"mask": {
    "uname": {
        "masking_value": "((PATIENTNAME))",
        "regex": None,
        "processing_func": None,
    },
    "upwd": {"masking_value": "((PASSWORD))", "regex": None, "processing_func": None},
    "ufname": {"masking_value": "((PATIENTFIRSTNAME))", "regex": None},
    "uminitial": {
        "masking_value": "((PATIENTMIDDLENAME))",
        "regex": None,
        "processing_func": None,
    },
    "ulname": {
        "masking_value": "((PATIENTLASTNAME))",
        "regex": None,
        "processing_func": None,
    },
    "upaddress": {
        "masking_value": "((ADDRESS))",
        "regex": None,
        "processing_func": None,
    },
    "upcity": {"masking_value": "((CITY))", "regex": None, "processing_func": None},
    "upPhone": {"masking_value": "((PHONENUMBER))", "regex": None},
    "zipcode": {"masking_value": "((ZIPCODE))", "regex": None, "processing_func": None},
    
    "upaddress2": {"masking_value": "((ADDRESS))", "regex": None},
    "initials": {
        "masking_value": "((PATIENTNAME))",
        "regex": None,
        "processing_func": None,
    },
    "vmid": {"masking_value": "((VMID))", "regex": None, "processing_func": None},
    "upreviousname": {"masking_value": "((PATIENTNAME))", "regex": None},
    "employername": {"masking_value": "((EMPLOYERNAME))", "regex": None},
    "employeraddress": {"masking_value": "((ADDRESS))", "regex": None},
    "employeraddress2": {"masking_value": "((ADDRESS))", "regex": None},
    "employercity": {
        "masking_value": "((CITY))",
        "regex": None,
        "processing_func": None,
    },
    "employerzip": {
        "masking_value": "((ZIPCODE))",
        "regex": None,
        "processing_func": None,
    },
    "employerPhone": {"masking_value": "((PHONENUMBER))", "regex": None},
    "insname": {
        "masking_value": "((INSURANCENAME))",
        "regex": None,
        "processing_func": None,
    },
    "pharmacyname": {
        "masking_value": "((PHARMACYNAME))",
        "regex": None,
        "processing_func": None,
    },
    "insname2": {
        "masking_value": "((INSURANCENAME))",
        "regex": None,
        "processing_func": None,
    },
    "patient_id": {
        "masking_value": "((patient_id))",
        "regex": None,
        "processing_func": None,
    },
    "encounter_id": {
        "masking_value": "((encounter_id))",
        "regex": None,
        "processing_func": None,
    },
    },
    "combine": {
        "patientfullname": {
            "combine": ["ufname", "uminitial", "ulname"],
            "masking_value": "((PATIENTNAME))",
            "regex": None,
            "processing_func": None,
        },
    },
    "dob": {
        "dob": {"masking_value": "((DOB))", "regex": None, "processing_func": None},
        "ptDob": {"masking_value": "((ptDob))", "regex": None, "processing_func": None},
    },
    "regex": {
        "uemail": {
            "masking_value": "((EMAIL))",
            "regex": [r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"],
            "processing_func": None,
        },
        "ssn": {
            "masking_value": "((SSN))",
            "regex": [r"\b\d{3}[- ]\d{2}[- ]\d{4}\b"],
            "processing_func": None,
        },
        "umobileno": {
            "masking_value": "((PHONENUMBER))",
            "regex": [
                r"\(\d{3}\)\s*\d{3}-\d{4}",  # (XXX) XXX-XXXX
                r"\b\d{3}-\d{3}-\d{4}\b",  # XXX-XXX-XXXX
                r"\+\d{1,3}\s\d{1,4}[- ]\d{3}[- ]\d{4}\b",  # International format
                r"\b\d{3} \d{3} \d{4}\b",  # XXX XXX XXXX (with spaces)
            ],
            "processing_func": None,
        },
    }
}
##################################################################