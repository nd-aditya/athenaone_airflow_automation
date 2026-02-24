import pandas as pd
import re
from collections import OrderedDict
from typing import List, Dict, Optional, Any
from datetime import datetime
from core.process_df.utils import PatientIdentifierType
try:
    from dateutil import parser as dateparser
except ImportError:
    dateparser = None
from presidio_analyzer import AnalyzerEngine
from deIdentification.nd_logger import nd_logger


analyzer = AnalyzerEngine()
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
    before_date = parse_date(str(before_date_str))
    after_date = parse_date(str(after_date_str))
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


class UnstructuredDetector:
    def __init__(self, columns_names: list[str], pii_config: dict, default_offset: int):
        self.columns_names = columns_names
        self.pii_config = pii_config
        self.default_offset = default_offset

        

    def extract_dates_from_text(
        self, text: str, normalize: bool = True, unique: bool = True
    ) -> List[Dict[str, Optional[str]]]:
        nd_logger.debug(f"Extracting dates from text: {text!r}")
        if not isinstance(text, str) or not text:
            nd_logger.debug("No text provided or text is not string.")
            return []
        matches = []
        seen = OrderedDict()
        for m in MASTER_PATTERN.finditer(text):
            raw = next(g for g in m.groups() if g)
            key = raw if unique else f"{raw}_{m.start()}"
            if unique and key in seen:
                continue
            norm = _try_parse(raw) if normalize else None
            item = {
                "match": raw,
                "normalized": norm,
                "start": m.start(),
                "end": m.end(),
            }
            nd_logger.debug(f"Found date match: {item}")
            if unique:
                seen[key] = item
            else:
                matches.append(item)
        result = list(seen.values()) if unique else matches
        nd_logger.debug(f"Extracted dates: {result}")
        return result

    def _exact_match(self, text, pii_data):
        nd_logger.debug(f"Checking for exact PII matches in text: {text}")
        result = [
            value
            for key, value in pii_data.items()
            if value and str(value) in str(text)
        ]
        nd_logger.debug(f"Exact PII matches found: {result}")
        return result

    def _presidio_analyzer(self, text, pii_data):
        nd_logger.debug(f"Running presidio analyzer on text: {text!r}")
        results = analyzer.analyze(
            text, entities=["PHONE_NUMBER", "EMAIL_ADDRESS", "PERSON"], language="en"
        )
        flagged = []
        for res in results:
            snippet = text[res.start : res.end]
            etype = res.entity_type
            nd_logger.debug(f"Presidio found entity: {etype} snippet: {snippet}")
            if etype == "PERSON" and pii_data:
                if is_pii_person(snippet, pii_data):
                    flagged.append((etype, snippet))
                    nd_logger.info(f"Person PII found by presidio and confirmed by checker: {snippet}")
            else:
                flagged.append((etype, snippet))
                nd_logger.info(f"Non-person PII found by presidio: {etype}: {snippet}")
        nd_logger.debug(f"Presidio flagged entities: {flagged}")
        return flagged

    def qc_replace_value(self, before_rows: pd.DataFrame, after_rows: pd.DataFrame):
        replace_values = self.pii_config.get("replace_values", [])

        qc_results = {"passed_rows": 0, "failed_rows": 0, "fail_details": []}
        nd_logger.info(f"Beginning QC replace value check")
        for idx, (before_row, after_row) in enumerate(
            zip(before_rows.itertuples(index=False), after_rows.itertuples(index=False))
        ):
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
                nd_logger.debug(f"[Row {idx}] {key_str=} {value_str=} {before_key_count=} {after_value_count=} {after_key_count=}")
                if before_key_count > after_value_count:
                    row_failed = True
                    fail_detail = {
                        "row_index": idx,
                        "mapping": {key: value},
                        "reason": (
                            f"Replacement value '{value_str}' appears {after_value_count} times, "
                            f"but original key '{key_str}' appears {before_key_count} times in before_text."
                        ),
                    }
                    nd_logger.warning(f"QC replacement check failed: {fail_detail}")
                    qc_results["fail_details"].append(fail_detail)
                if after_key_count > 0:
                    row_failed = True
                    fail_detail = {
                        "row_index": idx,
                        "mapping": {key: value},
                        "reason": f"Original key '{key_str}' still present {after_key_count} times in after_text.",
                    }
                    nd_logger.warning(f"QC replacement check failed: {fail_detail}")
                    qc_results["fail_details"].append(fail_detail)
            if row_failed:
                qc_results["failed_rows"] += 1
            else:
                qc_results["passed_rows"] += 1
        nd_logger.info(f"QC replace value summary: {qc_results}")
        return qc_results

    def is_deidentified(
        self,
        before_df: pd.DataFrame,
        after_df: pd.DataFrame,
        patient_id_cols: list[str],
        encounter_id_cols: list[str],
        appointment_id_cols: list[str],
        patient_mapping_df: pd.DataFrame,
        pii_data_df: pd.DataFrame,
        secondary_pii_dfs: list[pd.DataFrame],
        global_data_df: pd.DataFrame,
        enc_to_nd_pid_mapping_df: pd.DataFrame,
        appointment_to_nd_pid_mapping_df: pd.DataFrame
    ):

        nd_logger.info("Beginning deidentification QC process for unstructured columns.")
        final_qc_results = {}

        for notes_column in self.columns_names:
            nd_logger.info(f"Processing column: {notes_column}")
            if after_df.empty:
                nd_logger.warning(f"After dataframe is empty for column '{notes_column}'. All rows are failed.")
                all_remarks = {
                    "exact_match": [],
                    "presidio_entities": [],
                    "incorrect_masking": [],
                    "date_offset_failed": [],
                    "dob_remarks": {},
                }
                final_qc_results[notes_column] = {
                    "passed_count": 0,
                    "failed_count": before_df.shape[0],
                    "remarks": all_remarks,
                }
                continue

            replace_values_matches = self.qc_replace_value(before_df, after_df)
            nd_logger.debug(f"Replace values matches for column '{notes_column}': {replace_values_matches}")

            def check_row(row):
                before_text = str(row["before"])
                after_text = str(row["after"])
                nd_patient_id = row["nd_patient_id"]

                row_remarks = {
                    "exact_match": [],
                    "presidio_entities": [],
                    "incorrect_masking": [],
                    "date_offset_failed": [],
                    "dob_remarks": {},
                }

                # -------------------------
                # Get patient-specific data
                # -------------------------
                patient_pii_rows = pii_data_df[
                    pii_data_df["nd_patient_id"] == nd_patient_id
                ]

                # List of filtered secondary PII dfs for this patient
                patient_secondary_piis = [
                    snd_df[snd_df["nd_patient_id"] == nd_patient_id]
                    for snd_df in secondary_pii_dfs
                ]

                patient_mapping_row = patient_mapping_df[
                    patient_mapping_df["nd_patient_id"] == nd_patient_id
                ]

                # -------------------------
                # Get offset for this patient
                # -------------------------
                offset_value = self.default_offset
                if not patient_mapping_row.empty:
                    offset_value = patient_mapping_row.iloc[0]["offset"]
                nd_logger.debug(f"Patient {nd_patient_id} offset: {offset_value}")

                # -------------------------
                # Combine all PII data for this patient (multiple rows possible)
                # -------------------------
                combined_pii_data = {}

                # Primary PII
                for _, pii_row in patient_pii_rows.iterrows():
                    for col, val in pii_row.items():
                        if col not in ["nd_patient_id"] and pd.notna(val) and val != "":
                            combined_pii_data.setdefault(col, []).append(str(val))

                # Secondary PII (can have different columns)
                for sec_df in patient_secondary_piis:
                    for _, sec_row in sec_df.iterrows():
                        for col, val in sec_row.items():
                            if col not in ["nd_patient_id"] and pd.notna(val) and val != "":
                                combined_pii_data.setdefault(col, []).append(str(val))

                flattened_pii_data = {
                    k: v[0] if v else "" for k, v in combined_pii_data.items()
                }
                nd_logger.debug(f"Flattened PII data for patient {nd_patient_id}: {flattened_pii_data}")

                # -------------------------
                # Add global PII data
                # -------------------------
                global_pii_data = {}
                for _, global_row in global_data_df.iterrows():
                    for col, val in global_row.items():
                        if pd.notna(val) and val != "":
                            global_pii_data[col] = str(val)
                nd_logger.debug(f"Global PII data: {global_pii_data}")

                # -------------------------
                # Merge all PII sources
                # -------------------------
                all_pii_data = {**flattened_pii_data, **global_pii_data}
                nd_logger.debug(f"All PII data (merged): {all_pii_data}")

                # -------------------------
                # 1. Exact match check
                # -------------------------
                exact_matches = []

                for key, value in all_pii_data.items():
                    if value and str(value) in after_text:
                        exact_matches.append(value)

                for key, val_list in combined_pii_data.items():
                    for val in val_list:
                        if val and val in after_text:
                            exact_matches.append(val)

                row_remarks["exact_match"] = list(set(exact_matches))
                if row_remarks["exact_match"]:
                    nd_logger.warning(f"Exact PII matches found in after_text for patient {nd_patient_id}: {row_remarks['exact_match']}")

                # -------------------------
                # 2. Presidio Entities
                # -------------------------
                presidio_entities = self._presidio_analyzer(
                    after_text, all_pii_data
                )
                row_remarks["presidio_entities"] = presidio_entities
                if presidio_entities:
                    nd_logger.warning(f"Presidio entities found in after_text for patient {nd_patient_id}: {presidio_entities}")

                # -------------------------
                # 3. Date logic with DOB check
                # -------------------------
                dob_cols = self.pii_config.get("dob", [])
                dob_values = []
                for col in dob_cols:
                    if col in combined_pii_data:
                        dob_values.extend(combined_pii_data[col])
                    elif col in flattened_pii_data:
                        dob_values.append(flattened_pii_data[col])

                if dob_values:
                    dob_value = get_best_clean_dob(dob_values)
                    nd_logger.debug(f"Using DOB value: {dob_value}")
                    try:
                        normalise_dob = self.extract_dates_from_text(dob_value)[0][
                            "normalized"
                        ]
                        dob_year = get_year(normalise_dob)
                        nd_logger.debug(f"DOB normalized: {normalise_dob}, year: {dob_year}")
                    except (IndexError, AttributeError) as e:
                        nd_logger.error(f"Error parsing DOB for patient {nd_patient_id}: {e}")
                        normalise_dob = None
                        dob_year = None
                else:
                    normalise_dob = None
                    dob_year = None

                # -------------------------
                # Date offset validation
                # -------------------------
                before_dates = self.extract_dates_from_text(before_text)
                after_dates = self.extract_dates_from_text(after_text)
                nd_logger.debug(f"Before dates: {before_dates}")
                nd_logger.debug(f"After dates: {after_dates}")
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

                    # Handle DOB
                    if normalise_dob and bd == normalise_dob:
                        dob_values_count += 1
                        if ad == normalise_dob:
                            dob_failed_row = True
                            dob_reason += "DOB not properly masked"
                            nd_logger.warning(f"DOB not properly masked for patient {nd_patient_id} in row.")
                        if (
                            dob_year
                            and str(dob_year)
                            not in after_text[prev_date_indx:ad_start_index]
                        ):
                            dob_failed_row = True
                            dob_reason += (
                                "\nYear value not found in deidentified text for DOB"
                            )
                            nd_logger.warning(f"Year value for DOB not found in after_text [{prev_date_indx}:{ad_start_index}] for patient {nd_patient_id}")
                        prev_date_indx = after_dates[j]["end"]
                        i += 1
                        continue

                    if not is_date_offset_valid(bd, ad, offset_value):
                        fail_date_detail = {"before": bd, "after": ad, "expected_offset": int(offset_value)}
                        failed_dates.append(fail_date_detail)
                        nd_logger.warning(f"Date offset failed for patient {nd_patient_id}: {fail_date_detail}")

                    i += 1
                    j += 1

                expected_after_count = len(before_dates) - dob_values_count
                if expected_after_count != len(after_dates):
                    fail_count_detail = {
                        "before_count": len(before_dates),
                        "after_count": len(after_dates),
                        "dob_count": dob_values_count,
                        "reason": "Date count mismatch",
                    }
                    failed_dates.append(fail_count_detail)
                    nd_logger.warning(f"Date count mismatch for patient {nd_patient_id}: {fail_count_detail}")

                if failed_dates:
                    row_remarks["date_offset_failed"].extend(failed_dates)
                if dob_failed_row:
                    row_remarks["dob_remarks"] = {"failed": True, "reason": dob_reason}
                    nd_logger.warning(f"DOB masking failed for patient {nd_patient_id}: {dob_reason}")

                return row_remarks

            # Logging how pairing is being done
            if len(patient_id_cols)>0:
                nd_logger.info(f"Pairing rows using patient_id_cols: {patient_id_cols}")
                pair_df = pd.DataFrame(
                    {
                        "before": before_df[notes_column],
                        "after": after_df[notes_column],
                        "nd_patient_id": after_df[patient_id_cols[0]],
                    }
                )
            elif len(encounter_id_cols)>0:
                enc_mapping = dict(zip(enc_to_nd_pid_mapping_df["encounter_id"], enc_to_nd_pid_mapping_df["nd_patient_id"]))
                nd_logger.info(f"Pairing rows using encounter_id_cols {encounter_id_cols[0]} with provided mapping ({len(enc_mapping)} mappings)")
                pair_df = pd.DataFrame(
                    {
                        "before": before_df[notes_column],
                        "after": after_df[notes_column],
                        "nd_patient_id": before_df[encounter_id_cols[0]].map(enc_mapping),
                    }
                )
            elif len(appointment_id_cols)>0:
                appt_mapping = dict(zip(appointment_to_nd_pid_mapping_df["appointment_id"], appointment_to_nd_pid_mapping_df["nd_patient_id"]))
                nd_logger.info(f"Pairing rows using appointment_id_cols {appointment_id_cols[0]} with provided mapping ({len(appt_mapping)} mappings)")
                pair_df = pd.DataFrame(
                    {
                        "before": before_df[notes_column],
                        "after": after_df[notes_column],
                        "nd_patient_id": before_df[appointment_id_cols[0]].map(appt_mapping),
                    }
                )
            else:
                nd_logger.info("Pairing rows with no patient/encounter/appointment id columns (set nd_patient_id=None).")
                pair_df = pd.DataFrame(
                    {
                        "before": before_df[notes_column],
                        "after": after_df[notes_column],
                        "nd_patient_id": None,
                    }
                )

            nd_logger.debug(f"Applying QC check_row to all records in pair_df for column {notes_column}")
            pair_df["remarks"] = pair_df.apply(check_row, axis=1)

            all_remarks = {
                "exact_match": [],
                "presidio_entities": [],
                "incorrect_masking": [],
                "date_offset_failed": [],
                "dob_remarks": {},
            }
            any_failed = False

            for rem in pair_df["remarks"]:
                if (
                    rem["exact_match"]
                    or rem["presidio_entities"]
                    or rem["date_offset_failed"]
                    or rem.get("dob_remarks", {}).get("failed", False)
                ):
                    any_failed = True

                all_remarks["exact_match"].extend(rem.get("exact_match", []))
                all_remarks["presidio_entities"].extend(
                    rem.get("presidio_entities", [])
                )
                all_remarks["incorrect_masking"].extend(
                    rem.get("incorrect_masking", [])
                )
                all_remarks["date_offset_failed"].extend(
                    rem.get("date_offset_failed", [])
                )

                if rem.get("dob_remarks"):
                    all_remarks["dob_remarks"].update(rem["dob_remarks"])

            all_remarks["replace_values_matches"] = replace_values_matches

            all_remarks["exact_match"] = list(set(all_remarks["exact_match"]))
            all_remarks["presidio_entities"] = list(
                set(all_remarks["presidio_entities"])
            )

            failed_count = int(any_failed * len(pair_df))
            passed_count = len(pair_df) - failed_count

            nd_logger.info(f"Final QC result for column '{notes_column}': {passed_count=}, {failed_count=}, remarks keys: {list(all_remarks.keys())}")
            final_qc_results[notes_column] = {
                "passed_count": passed_count,
                "failed_count": failed_count,
                "failure_reasons": [],
                "failure_nd_auto_incr_ids": [],
                "remarks": all_remarks
            }

        nd_logger.info("Deidentification QC process completed.")
        return final_qc_results
