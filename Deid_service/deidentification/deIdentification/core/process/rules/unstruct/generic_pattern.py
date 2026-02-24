import re
from .date_parse import cached_parse_date
from .utils import GENERIC_REGEX_DICT


class GenericPatternDeIdentification:
    def __init__(self, text: str, offset_value: int, date_parse_cache: dict):
        self.text = text
        self.offset_value = offset_value
        self.date_parse_cache = date_parse_cache  # Cache for parsed dates

    def date(self):
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
            
    
    def others(self):
        # These are simple patterns that don't require date parsing.
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
                        "number": "((HouseNumber))",
                        "street": "((StreetName))",
                        "city": "((City))",
                        "state": "((State))",
                        "zip": "((ZIPCODE))",
                    }

                    pattern = re.compile(ad_regex, re.VERBOSE | re.IGNORECASE)
                    result = text
                    
                    # Find all matches in the text
                    matches = list(pattern.finditer(text))
                    
                    # Process matches in reverse order to avoid offset issues
                    for match in reversed(matches):
                        # Extract the ZIP code and truncate it to the first three digits
                        zip_code = match.group('zip')[:3]
                        
                        # Get the matched text
                        matched_text = match.group(0)
                        
                        # Determine if this is a PO Box address
                        is_po_box = bool(re.search(r'P\.?[Oo]\.?\s*Box', matched_text, re.IGNORECASE))
                        
                        # Construct the masked address
                        if is_po_box:
                            masked_address = f"((HouseNumber)), ((City)), ((State)) {zip_code}"
                        else:
                            masked_address = f"((HouseNumber)) ((StreetName)), ((City)), ((State)) {zip_code}"
                        
                        # Replace just the matched portion with the masked address
                        start, end = match.span()
                        result = result[:start] + masked_address + result[end:]
                    
                    return result

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
                    try:
                        self.text = matches.sub(value["masking_value"], self.text)
                    except:
                        pass

    def deidentify(self) -> str:
        self.date()
        self.others()
        return self.text
