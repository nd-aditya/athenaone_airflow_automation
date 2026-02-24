import pandas as pd
import re
from collections import OrderedDict
from typing import List, Dict, Optional, Any
from datetime import datetime

try:
    from dateutil import parser as dateparser
except ImportError:
    dateparser = None

MONTHS = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)

DATE_REGEXES = [
    rf"\b{MONTHS}\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,\s*\d{{2,4}})?\b",
    rf"\b\d{{1,2}}(?:st|nd|rd|th)?\s+{MONTHS}(?:,?\s*\d{{2,4}})?\b",
    r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:\d{2})?)?\b",
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    r"\b\d{1,2}-\d{1,2}-\d{2,4}\b",
    r"\b\d{1,2}\.\d{1,2}\.\d{2,4}\b",
    r"\b\d{8}(?:\d{6})?\b",
    rf"\b\d{{1,2}}(?:st|nd|rd|th)?\s+{MONTHS},\s*\d{{2,4}}\b",
    rf"\b{MONTHS}\s+\d{{4}}\b",
]
MASTER_PATTERN = re.compile("|".join(f"({pat})" for pat in DATE_REGEXES), re.IGNORECASE)

def _try_parse(dt_str: str) -> Optional[str]:
    if not dateparser:
        return None
    try:
        dt = dateparser.parse(dt_str, dayfirst=False, fuzzy=True)
        if dt is None:
            return None
        if (dt.hour, dt.minute, dt.second, dt.microsecond) != (0, 0, 0, 0):
            return dt.isoformat()
        return dt.date().isoformat()
    except Exception:
        return None

def parse_date(date_str):
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def is_date_offset_valid(before_date_str, after_date_str, offset_days):
    before_date = parse_date(before_date_str)
    after_date = parse_date(after_date_str)
    if not before_date or not after_date:
        return False
    delta = abs((after_date - before_date).days)
    return delta == offset_days

def get_year(date_str):
    dob = dateparser.parse(date_str)
    year = dob.year
    return year

def is_pii_person(candidate: str, pii_info: dict):
    name_fields = [v.lower() for k, v in pii_info.items() if "name" in k.lower()]
    c = candidate.lower()
    for nf in name_fields:
        if nf and (nf == c or nf in c or c in nf):
            return True
        nf_parts = nf.split()
        if any(part in c for part in nf_parts if len(part) > 2):
            return True
    return False

def get_best_clean_dob(dob_values: List[str]):
    return dob_values[0]


class UnstructuredDetectorDF:
    def __init__(self, columns_names, pii_config):
        self.columns_names = columns_names
        self.pii_config = pii_config

        from presidio_analyzer import AnalyzerEngine
        self.analyzer = AnalyzerEngine()

    def extract_dates_from_text(
        self, text: str, normalize: bool = True, unique: bool = True
    ) -> List[Dict[str, Optional[str]]]:
        if not isinstance(text, str) or not text:
            return []
        matches = []
        seen = OrderedDict()
        for m in MASTER_PATTERN.finditer(text):
            raw = next(g for g in m.groups() if g)
            key = raw if unique else f"{raw}_{m.start()}"
            if unique and key in seen:
                continue
            norm = _try_parse(raw) if normalize else None
            item = {"match": raw, "normalized": norm, "start": m.start(), "end": m.end()}
            if unique:
                seen[key] = item
            else:
                matches.append(item)
        return list(seen.values()) if unique else matches

    # def _exact_match(self, text, pii_data):
    #     return [value for key, value in pii_data.items() if value and str(value) in str(text)]

    def _presidio_analyzer(self, text, pii_data):
        results = self.analyzer.analyze(
            text, entities=["PHONE_NUMBER", "EMAIL_ADDRESS", "PERSON"], language="en"
        )
        flagged = []
        for res in results:
            snippet = text[res.start : res.end]
            etype = res.entity_type
            if etype == "PERSON" and pii_data:
                if is_pii_person(snippet, pii_data):
                    flagged.append((etype, snippet))
            else:
                flagged.append((etype, snippet))
        return flagged

    def qc_replace_value(self, before_rows: pd.DataFrame, after_rows: pd.DataFrame):
        replace_values = self.pii_config.get("replace_values", [])

        qc_results = {"passed_rows": 0, "failed_rows": 0, "fail_details": []}
        for idx, (before_row, after_row) in enumerate(zip(before_rows.itertuples(index=False), after_rows.itertuples(index=False))):
            if isinstance(before_row, tuple):
                before_text = str(getattr(before_row, self.columns_names[0]))
            else:
                before_text = str(before_row)
            if isinstance(after_row, tuple):
                after_text = str(getattr(after_row, self.columns_names[0]))
            else:
                after_text = str(after_row)
            row_failed = False
            for replace_vdict in replace_values:
                key, value = next(iter(replace_vdict.items()))
                key_str = str(key)
                value_str = str(value)
                before_key_count = before_text.count(key_str)
                after_value_count = after_text.count(value_str)
                after_key_count = after_text.count(key_str)
                if before_key_count > after_value_count:
                    row_failed = True
                    qc_results["fail_details"].append(
                        {
                            "row_index": idx,
                            "mapping": {key: value},
                            "reason": (
                                f"Replacement value '{value_str}' appears {after_value_count} times, "
                                f"but original key '{key_str}' appears {before_key_count} times in before_text."
                            ),
                        }
                    )
                if after_key_count > 0:
                    row_failed = True
                    qc_results["fail_details"].append(
                        {
                            "row_index": idx,
                            "mapping": {key: value},
                            "reason": f"Original key '{key_str}' still present {after_key_count} times in after_text.",
                        }
                    )
            if row_failed:
                qc_results["failed_rows"] += 1
            else:
                qc_results["passed_rows"] += 1
        return qc_results

    def is_deidentified(self, before_df: pd.DataFrame, after_df: pd.DataFrame, patient_id_column: str, patient_mapping_df: pd.DataFrame, global_data_df: pd.DataFrame, pii_data_df: pd.DataFrame):
    
        final_qc_results = {}
        
        for notes_column in self.columns_names:
            # Batch replace_values QC, once per column
            replace_values_matches = self.qc_replace_value(before_df, after_df)
            
            def check_row(row):
                before_text = str(row['before'])
                after_text = str(row['after'])
                nd_patient_id = row['nd_patient_id']
                
                row_remarks = {
                    "exact_match": [],
                    "presidio_entities": [],
                    "incorrect_masking": [],
                    "date_offset_failed": [],
                    "dob_remarks": {}
                }
                
                # Get patient-specific data
                patient_pii_rows = pii_data_df[pii_data_df['nd_patient_id'] == nd_patient_id]
                patient_mapping_row = patient_mapping_df[patient_mapping_df["nd_patient_id"] == nd_patient_id]
                
                # Get offset for this patient
                offset_value = 30  # default
                if not patient_mapping_row.empty:
                    offset_value = patient_mapping_row.iloc[0]['offset']
                
                # Combine all PII data for this patient (multiple rows possible)
                combined_pii_data = {}
                for _, pii_row in patient_pii_rows.iterrows():
                    for col, val in pii_row.items():
                        if col != "nd_patient_id" and pd.notna(val) and val != "":
                            if col not in combined_pii_data:
                                combined_pii_data[col] = []
                            combined_pii_data[col].append(str(val))
                
                # Flatten the lists to single values for compatibility
                flattened_pii_data = {}
                for key, val_list in combined_pii_data.items():
                    flattened_pii_data[key] = val_list[0] if val_list else ""
                
                # Add global data (common for all patients)
                global_pii_data = {}
                for _, global_row in global_data_df.iterrows():
                    for col, val in global_row.items():
                        if pd.notna(val) and val != "":
                            global_pii_data[col] = str(val)
                
                # Combine patient-specific and global PII data
                all_pii_data = {**flattened_pii_data, **global_pii_data}
                
                # 1. Exact match check
                exact_matches = []
                for key, value in all_pii_data.items():
                    if value and str(value) in after_text:
                        exact_matches.append(value)
                # Also check against all values in combined_pii_data lists
                for key, val_list in combined_pii_data.items():
                    for val in val_list:
                        if val and val in after_text:
                            exact_matches.append(val)
                row_remarks["exact_match"] = list(set(exact_matches))
                
                # 2. Presidio Entities
                row_remarks["presidio_entities"] = self._presidio_analyzer(after_text, all_pii_data)
                
                # 3. Date logic with patient-specific DOB
                dob_cols = self.pii_config.get('dob', [])
                dob_values = []
                for col in dob_cols:
                    if col in combined_pii_data:
                        dob_values.extend(combined_pii_data[col])
                    elif col in flattened_pii_data:
                        dob_values.append(flattened_pii_data[col])
                
                if dob_values:
                    dob_value = get_best_clean_dob(dob_values)
                    try:
                        normalise_dob = self.extract_dates_from_text(dob_value)[0]["normalized"]
                        dob_year = get_year(normalise_dob)
                    except (IndexError, AttributeError):
                        normalise_dob = None
                        dob_year = None
                else:
                    normalise_dob = None
                    dob_year = None
                
                # Date offset validation
                before_dates = self.extract_dates_from_text(before_text)
                after_dates = self.extract_dates_from_text(after_text)
                i, j = 0, 0
                prev_date_indx = 0
                failed_dates = []
                dob_failed_row = False
                dob_reason = ""
                dob_values_count = 0
                
                while i < len(before_dates) and j < len(after_dates):
                    bd = before_dates[i]["normalized"]
                    ad = after_dates[j]["normalized"]
                    ad_start_index = after_dates[j]["start"]
                    
                    # Handle DOB specifically
                    if normalise_dob and bd == normalise_dob:
                        dob_values_count += 1
                        if ad == normalise_dob:
                            dob_failed_row = True
                            dob_reason += "DOB not properly masked"
                        if dob_year and str(dob_year) not in after_text[prev_date_indx:ad_start_index]:
                            dob_failed_row = True
                            dob_reason += "\nYear value not found in deidentified text for DOB"
                        prev_date_indx = after_dates[j]["end"]
                        i += 1
                        continue
                    
                    # Check date offset for non-DOB dates
                    if not is_date_offset_valid(bd, ad, offset_value):
                        failed_dates.append({'before': bd, 'after': ad, 'expected_offset': offset_value})
                    i += 1
                    j += 1
                
                # Check date count mismatch
                expected_after_count = len(before_dates) - dob_values_count
                if expected_after_count != len(after_dates):
                    failed_dates.append({
                        "before_count": len(before_dates),
                        "after_count": len(after_dates),
                        "dob_count": dob_values_count,
                        "reason": "Date count mismatch",
                    })
                
                if failed_dates:
                    row_remarks["date_offset_failed"].extend(failed_dates)
                if dob_failed_row:
                    row_remarks["dob_remarks"] = {"failed": True, "reason": dob_reason}
                
                return row_remarks
            
            # Create pair DataFrame with patient IDs
            pair_df = pd.DataFrame({
                'before': before_df[notes_column],
                'after': after_df[notes_column],
                'nd_patient_id': before_df[patient_id_column]
            })
            
            # Apply check_row function
            pair_df['remarks'] = pair_df.apply(check_row, axis=1)
            
            # Aggregate results across all rows
            all_remarks = {
                "exact_match": [],
                "presidio_entities": [],
                "incorrect_masking": [],
                "date_offset_failed": [],
                "dob_remarks": {}
            }
            
            any_failed = False
            for idx, rem in enumerate(pair_df['remarks']):
                if rem["exact_match"] or rem["presidio_entities"] or rem["date_offset_failed"] or rem.get("dob_remarks", {}).get("failed", False):
                    any_failed = True
                
                all_remarks["exact_match"].extend(rem.get("exact_match", []))
                all_remarks["presidio_entities"].extend(rem.get("presidio_entities", []))
                all_remarks["incorrect_masking"].extend(rem.get("incorrect_masking", []))
                all_remarks["date_offset_failed"].extend(rem.get("date_offset_failed", []))
                
                if rem.get("dob_remarks"):
                    all_remarks["dob_remarks"].update(rem["dob_remarks"])
            
            # Add replace values matches
            all_remarks["replace_values_matches"] = replace_values_matches
            
            # Deduplicate
            all_remarks["exact_match"] = list(set(all_remarks["exact_match"]))
            all_remarks["presidio_entities"] = list(set(all_remarks["presidio_entities"]))
            
            failed_count = int(any_failed * len(pair_df))
            passed_count = len(pair_df) - failed_count
            
            final_qc_results[notes_column] = {
                "passed_count": passed_count,
                "failed_count": failed_count,
                "remarks": all_remarks
            }
        
        return final_qc_results


# --- Sample usage with your example---
before_rows = pd.DataFrame([
    {
        "notes": "1986-04-12, Texas, In facility Texas Patient Jessica Marie Thompson with patient-id: 1001 was admitted on January 1, 1986 and July 22, 2025. She is a 39-year-old woman.",
        "name": "rohit",
        "profile_id": 100
    }
])

after_rows = pd.DataFrame([
    {
        "notes": "In facility ((FacilityName)) Patient Jessica with patient-id: 10001101011 was admitted on January 31, 1986 and July 22, 2025. She is a 39-year-old woman.",
        "name": "rohit",
        "profile_id": 10111010100
    }
])
pii_data_df = pd.DataFrame([
    {
        "profile_id": 10111010100,
        "name": "Jessica Marie Thompson",
        "dob": "1986-04-12",
        "phone": "(415) 763-2184",
        "email": "jessica.thompson86@email.com",
    },
    {
        "profile_id": 10111010100,
        "name": None,
        "dob": None,
        "phone": "(515) 763-4321",
        "email": "rohit.thompson86@email.com",
    },
    {
        "profile_id": 10111020200,
        "name": None,
        "dob": None,
        "phone": "(543) 763-4381",
        "email": "rohit.chouhan@email.com",
    },
])
global_data_df = pd.DataFrame([
    {"col1_name": "Lonara", "col2_name": "Madhya Pradesh"},
    {"col1_name": "Jhirniya", "col2_name": "Khargone"},
])


pii_config = {
    "mask": {
        "name": "((FirstName))",
        "dob": "((dob))",
        "phone": "((phone))",
        "email": "((email))",
        "insurnce_id": "((insurnce_id))",
    },
    "dob": ["dob"],
    "replace_values": [{1001: 10001101011}, {"Texas": "((FacilityName))"}],
}

offset_value = 30
columns_names = ["notes"]
patient_id_column = "profile_id"
patient_mapping_df = pd.DataFrame([
    {
        "PATIENTID": 10111010100,
        "offset": 30,
    },
    {
        "PATIENTID": 10112010100,
        "offset": -34,
    },
])
detector = UnstructuredDetectorDF(columns_names, pii_config=pii_config)
qc_result = detector.is_deidentified(before_rows, after_rows, patient_id_column=patient_id_column, patient_mapping_df=patient_mapping_df, global_data_df=global_data_df, pii_data_df=pii_data_df)
print(qc_result)
