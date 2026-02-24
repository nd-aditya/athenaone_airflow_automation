import dateparser
from datetime import datetime, timedelta
from rapidfuzz import fuzz
from presidio_analyzer import AnalyzerEngine, RecognizerResult
from presidio_anonymizer import AnonymizerEngine
import regex as re
from dateutil import parser
# from .utils import *
LABELS_TO_IGNORE = [
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
class Text_Anonymizer:
    def __init__(self):
        # self.text = text
        # self.entities = entities
        pass

    def split_entities_groups(self, entities_group, entities_type):
        second_entities = [
            entity
            for entity in entities_group
            if entity["entity_type"] in entities_type
        ]
        first_entities = [
            entity
            for entity in entities_group
            if entity["entity_type"] not in entities_type
        ]

        return first_entities, second_entities

    def normalize_date_format(self, date_str):
        date_str = re.sub(r"[-.]", "/", date_str)

        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                month, day, year = parts
                if len(year) == 2:
                    year = "20" + year if int(year) < 50 else "19" + year
                    date_str = f"{month}/{day}/{year}"

        return date_str
    
    # def normalize_date_format(self, date_str):
    #     date_str = re.sub(r"[-.]", "/", date_str)

    #     if "/" in date_str:
    #         parts = date_str.split("/")
    #         if len(parts) == 3:
    #             month, day, year = parts
    #             if len(year) == 2:
    #                 year = "20" + year if int(year) < 50 else "19" + year
    #                 date_str = f"{month}/{day}/{year}"

    #     return date_str

    def is_month_year_format(self, date_str):

        date_str = re.sub(r"[,\s]", "", date_str)
        pattern = r"^(\d{1,2})[-./](\d{4})$"
        match = re.match(pattern, date_str)

        if match:
            month_part = int(match.group(1))
            return True, month_part <= 12
        return False, False

    def parse_unstructured_date(self, date_str, offset=32):
        try:
            # Set a distant reference date to identify default value
            reference_date = datetime(5023, 1, 1)

            # Clean input string
            date_str = str(date_str).strip().lower()

            # Check for month-year format before normalization
            is_month_year, is_valid_month = self.is_month_year_format(date_str)
            
            if is_month_year:
                if not is_valid_month:
                    return "", ""  # Invalid month number (>2)
                # Parse without normalization to preserve the format
                parsed_date = dateparser.parse(
                    date_str,
                    settings={
                        "RELATIVE_BASE": reference_date,
                        "RETURN_AS_TIMEZONE_AWARE": False,
                        "DATE_ORDER": "MDY",
                    },
                )

                if parsed_date:
                    parsed_date = parsed_date + timedelta(days=offset)
                    return parsed_date.strftime("%B, %Y"), parsed_date.year
                return "", ""
            
            date_str = self.normalize_date_format(date_str)
            # print(f"3. Normalized: {date_str}")
            # Parse the date with custom settings
            parsed_date = dateparser.parse(
                date_str,
                settings={
                    "RELATIVE_BASE": reference_date,
                    "RETURN_AS_TIMEZONE_AWARE": False,
                    "DATE_ORDER": "MDY",
                },
            )
            
            if not parsed_date:
                return "", ""

            # Apply the offset
            parsed_date = parsed_date + timedelta(days=offset)

            # Check if the input contains only month (no day or year)
            # contains_only_month = all(str(i) not in date_str for i in range(1,32)) and not any(char.isdigit() for char in date_str)

            # Format the output based on input type
            if parsed_date.year == 5023:  # No year was specified
                # if contains_only_month:
                #     return parsed_date.strftime("%B"), ''  # Return only month
                # else:
                try:
                    return parsed_date.strftime("%B %d"), ""  # Return month and day
                except:
                    return "", ""
            else:
                # has_year = (
                #     any(str(year) in date_str for year in range(1900,2100)) or
                #     any(str(year) in date_str for year in range(0,100)) or   # Short year
                #     '/' in date_str or
                #     '-' in date_str
                # )
                pattern = re.compile(r"(19\d{2}|20\d{2}|\b\d{1,2}\b)")
                has_year = bool(pattern.search(date_str))

                # Check if the input had a year
                if has_year:
                    if re.search(r"\d+\s*,", date_str):
                        # Format is likely month day, year (e.g., "March 1, 2003")
                        return parsed_date.strftime("%B %d, %Y"), parsed_date.year

                    elif "," in date_str:
                        # Format for "month, year" pattern
                        return parsed_date.strftime("%B, %Y"), parsed_date.year
                    else:
                        # Format for full date pattern
                        return parsed_date.strftime("%B %d, %Y"), parsed_date.year
                else:
                    return parsed_date.strftime("%B %d"), parsed_date.year

        except:
            return "", ""

    def extract_year_old(self, date_string):
        date_string = date_string.strip()

        # match full date format with 4-digit year
        pattern1 = r"(?:,\s*)?([12]\d{3})(?!\d)$"
        match = re.search(pattern1, date_string)
        if match:
            return str(match.group(1))

        # 2-digit year at end
        pattern2 = "(?:,\s*)?(\d{2})(?!\d)$"
        match = re.search(pattern2, date_string)
        if match:
            year = match.group(1)
            return str("20" + year if int(year) < 24 else "19" + year)

        # Standalone 4 digit year
        pattern3 = r"^([12]\d{3})$"
        match = re.search(pattern3, date_string)
        if match:
            return str(match.group(1))

        return ""

    def extract_year(self, dob):
        if dob in ("00/00/0000", "0000-00-00"):
            return None
        try:
            # print("dob:", dob)
            return int(parser.parse(dob).year)
        except parser.ParserError:
            return None

    def change_date_text(self, text, date_entities, dob="", offset=32):
        # Initialize the dob_offset to None in case no dob is provided.
        dob_offset = None

        try:
            # If a date of birth (dob) is provided, parse it.
            if dob:
                dob_offset = self.parse_unstructured_date(dob, offset=offset)
                # print("DOB: ", dob_offset)

            # Prepare a dictionary to store date replacements.
            date_str_change = {}

            # Iterate over each date entity.
            for date_entity in date_entities:
                try:
                    # Parse the date from the entity word.
                    entity_date_res = self.parse_unstructured_date(
                        str(date_entity["word"]), offset=offset
                    )

                    # If dob_offset is not None and equals the entity's parsed date, use the year from dob_offset.
                    if dob_offset is not None and dob_offset == entity_date_res:
                        new_date = self.extract_year(
                            self.parse_unstructured_date(dob, offset=0)
                        )
                    else:
                        new_date = entity_date_res

                    # Store the mapping of the original date to the new date.
                    date_str_change[str(date_entity["word"])] = new_date

                except Exception as e:
                    # Log or handle the exception if parsing a date entity fails.
                    print(f"Error parsing date entity {date_entity}: {e}")
                    # Optionally, continue processing other entities or set a default value for new_date.
                    date_str_change[str(date_entity["word"])] = str(date_entity["word"])

            # Replace the original dates in the text with the new dates.
            for original_date, new_date in date_str_change.items():
                try:
                    # Replace the date in the text.
                    text = text.replace(original_date, new_date)
                except Exception as e:
                    # Handle any errors that occur during string replacement.
                    print(
                        f"Error replacing {original_date} with {new_date} in text: {e}"
                    )

        except Exception as e:
            # Handle any errors that occur in the overall process.
            print(f"An error occurred in change_date_text: {e}")

        return text

    def get_relevant_entities(self, entities, patient_name, score_threshold=75):
        ref_entity = ["((PATIENT))", "((PERSON))"]  # Relevant entity types
        relevant_entities = []  # List to store the relevant entities

        patient_name = patient_name.lower()  # Normalize the patient name to lowercase

        for entity in entities:
            # Check if the entity's type matches the relevant ones
            if entity["entity_type"] in ref_entity:
                # Calculate the fuzzy match score between the entity's word and the patient name
                fuzz_score = fuzz.partial_ratio(entity["word"].lower(), patient_name)
                # print(entity['word'].lower(), patient_name, fuzz_score)

                # If the score exceeds the threshold, add it to relevant_entities
                if fuzz_score > score_threshold:
                    # print(entity)  # Debug print to show the relevant entity
                    relevant_entities.append(entity)
            else:
                # Add non-relevant entities directly to the list
                relevant_entities.append(entity)

        return relevant_entities

    def remove_unwanted_entities(self, entities):
        return [
            entity
            for entity in entities
            if entity["entity_type"] not in LABELS_TO_IGNORE
        ]

    def get_recognizer_entities(self, entities):
        lst = []
        for entity in entities:
            entity_text = entity["word"]
            start = entity["start"]
            end = entity["end"]
            entity_type = entity["entity_type"]
            score = entity["score"]
            lst.append(RecognizerResult(entity_type, start, end, score))

        return lst

    def anonymizer(
        self, text, entities, patient_name, date_offset=31, dob="", score_threshold=75
    ):

        date_entities_type = ["<DATE_TIME>", "DATE", "DATE_TIME"]
        entities, date_entities = self.split_entities_groups(
            entities, date_entities_type
        )
        entities = self.remove_unwanted_entities(entities=entities)
        # print(f"*******filtered Entities********")
        # print(entities)
        # print("*******Date Entity********")
        # print(date_entities)

        if (patient_name is not None) or (patient_name != ""):
            entities = self.get_relevant_entities(
                entities=entities,
                patient_name=patient_name,
                score_threshold=score_threshold,
            )
        # print("*******Relevant Enities*******")
        # print(entities)

        anonymizer = AnonymizerEngine()
        anonymized_result = anonymizer.anonymize(
            text=text, analyzer_results=self.get_recognizer_entities(entities)
        )

        # print(f"Original Text: {text}")
        text = anonymized_result.text
        # print(f"Text before date change: {text}")
        text = self.change_date_text(
            text=text, date_entities=date_entities, dob=dob, offset=date_offset
        )
        # print(f"Reframed Text: {text}")
        return text


text_anonymizer = Text_Anonymizer()

def cached_parse_date(date_parse_cache, date_str, offset):
    # Check cache first
    key = (date_str.lower(), offset)
    if key in date_parse_cache:
        return date_parse_cache[key], date_parse_cache
    # Parse and cache result
    parsed_date = text_anonymizer.parse_unstructured_date(date_str, offset)
    date_parse_cache[key] = parsed_date
    return parsed_date, date_parse_cache
