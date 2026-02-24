import re
import itertools
from .date_parse import cached_parse_date
from .utils import GENERIC_DATE_REGEX, GENERIC_REGEX_DICT

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


class UnstructuredDeidentificationFinal:
    def __init__(
        self,
    ):
        self.date_parse_cache = {}
        
    def deidentify(self, notes_text: str, pii_data: dict, pii_config: dict, xml_config: dict, universal_pii_data: dict):
        if (not notes_text) or (len(notes_text) < 3):
            return notes_text
        
        for key, conf in pii_config.get("regex", {}).items():
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
        # for pii_name, conf in pii_config.get("combine", {}).items():
        #     mask_val = conf["masking_value"]
        #     full_name_lst = []
        #     name_parts = [
        #         str(pii_data[i])
        #         for i in conf["combine"]
        #         if pii_data.get(i) and len(pii_data.get(i)) > 2
        #     ]
        #     for r in range(1, len(name_parts) + 1):
        #         for combo in itertools.product(name_parts, repeat=r):
        #             combined_name = "".join(combo)  # Combine parts without space
        #             full_name_lst.append(combined_name)

        #     # Add each combination to the name_map with the appropriate masking value
        #     for i in full_name_lst:
        #         name_map[i.lower()] = mask_val
        for key, value in pii_config.get("mask", {}).items():
            mask_val = value["masking_value"]
            original_str = str(pii_data[key])
            if original_str and len(str(original_str)) > 2:
                # original_str = str(pii_data[key])
                mask_val = value["masking_value"]
                original_str = f"{original_str.strip()}" if original_str else ""
                name_map[original_str.lower()] = mask_val
        
        for key, conf in pii_config.get("dob", {}).items():
            mask_val = conf["masking_value"]
            original_str = str(pii_data[key])
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

        for replace_conf in pii_config.get("replace_value", []):
            old_value, new_value = replace_conf["old_value"], replace_conf["new_value"]
            pattern_id = r"\b{}\b".format(re.escape(str(old_value)))
            self.text = re.sub(pattern_id, str(new_value), self.text)


        date_pattern = GENERIC_REGEX_DICT["date"]["regex"]
        # Find all dates
        redg_date = re.compile(date_pattern, re.IGNORECASE | re.VERBOSE)
        # dates_found = re.findall(date_pattern, self.text, flags=re.IGNORECASE)
        dates_found = redg_date.findall(self.text)
        # Parse all dates and store in a map
        date_map = {}

        for d in dates_found:
            if d.lower() not in date_map:
                (d_offset, d_year), self.date_parse_cache = cached_parse_date(self.date_parse_cache, d, self.offset_value)
                # Use offset version if available
                replacement = d_offset if d_offset else d

                date_map[d.lower()] = replacement

        # Replace all found dates in a single pass
        if date_map:
            # Sort by length to handle longer matches first (optional)
            sorted_dates = sorted(date_map.keys(), key=len, reverse=True)
            date_pattern_all = re.compile(
                "|".join(re.escape(k) for k in sorted_dates), re.IGNORECASE
            )

            def date_repl_func(m):
                matched_lower = m.group(0).lower()
                return date_map[matched_lower]
            self.text = date_pattern_all.sub(date_repl_func, self.text)
        
        for key, value in GENERIC_REGEX_DICT.items():
            flag = 0
            if key == "date":
                continue
            if key == "driver_license":
                flag = 1
            if value.get("regex") and key == "address":
                patterns = (
                    value["regex"]
                    if isinstance(value["regex"], list)
                    else [value["regex"]]
                )

                def maskadd(text, ad_regex):
                    # Placeholder dictionary
                    placeholders = {
                        0: "<house number>",
                        1: "<street name>",
                        2: "<city>",
                        3: "<state>",
                        4: "<zipcode>",
                        5: "<zipcode>",
                    }
                    address_regex = re.compile(ad_regex, re.VERBOSE)
                    # Match and process
                    matches = address_regex.findall(text)
                    if matches:
                        replacements = [
                            (match[k], match[k][:3] if k == 4 else placeholders[k])
                            for match in matches
                            for k in range(len(match))
                            if len(match[k]) > 1 and k != 3
                        ]

                        # Apply replacements sequentially
                        for old, new in replacements:
                            text = text.replace(old, new)

                    return text

                for pat in patterns:
                    self.text = maskadd(self.text, pat)
                    # print(self.text)
            if value.get("regex") and key != "address":
                patterns = (
                    value["regex"]
                    if isinstance(value["regex"], list)
                    else [value["regex"]]
                )
                for pat in patterns:
                    if flag == 1:
                        matches = re.compile(pat)
                    else:
                        matches = re.compile(pat, re.IGNORECASE)
                    self.text = matches.sub(value["masking_value"], self.text)