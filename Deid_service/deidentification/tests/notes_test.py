from presidio_analyzer import AnalyzerEngine
from datetime import datetime, timedelta
import re
from collections import OrderedDict
from typing import List, Dict, Optional
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
    # January 1, 1986 / July 22, 2025 / Jul 22, 25
    rf"\b{MONTHS}\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,\s*\d{{2,4}})?\b",
    # 1 January 1986 / 22 Jul 2025ff
    rf"\b\d{{1,2}}(?:st|nd|rd|th)?\sf+{MONTHS}(?:,?\s*\d{{2,4}})?\b",
    # ISO 8601: 2025-07-22, 2025-07-22T10:30, 2025-07-22 10:30:15+05:30, etc.
    r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:\d{2})?)?\b",
    # 07/22/2025, 7/2/25
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    # 22-07-2025, 2-7-25
    r"\b\d{1,2}-\d{1,2}-\d{2,4}\b",
    # 22.07.2025
    r"\b\d{1,2}\.\d{1,2}\.\d{2,4}\b",
    # Compact yyyymmdd or yyyymmddHHMMSS
    r"\b\d{8}(?:\d{6})?\b",
    # 01 Jan, 1986  (comma between day and year)
    rf"\b\d{{1,2}}(?:st|nd|rd|th)?\s+{MONTHS},\s*\d{{2,4}}\b",
    # Month YYYY (e.g., January 1986)
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


def get_best_clean_dob(dob_values: dict[str]):
    return dob_values[0]


class UnstructuredDetector:
    def __init__(self, columns_names, pii_config):
        self.columns_names = columns_names
        self.pii_config = pii_config
        self.analyzer = AnalyzerEngine()

    def extract_dates_from_text(
        self, text: str, normalize: bool = True, unique: bool = True
    ) -> List[Dict[str, Optional[str]]]:
        matches = []
        seen = OrderedDict()
        for m in MASTER_PATTERN.finditer(text):
            # one of the groups will be not None
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
            if unique:
                seen[key] = item
            else:
                matches.append(item)
        return list(seen.values()) if unique else matches

    def _exact_match(self, text, pii_data):
        found_pii_values = []
        for key, value in pii_data.items():
            if value and value in text:
                found_pii_values.append(value)
        return found_pii_values

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

    def qc_replace_value(self, before_rows, after_rows):
        replace_values = self.pii_config.get("replace_values", [])

        qc_results = {"passed_rows": 0, "failed_rows": 0, "fail_details": []}

        for idx, (before_item, after_item) in enumerate(zip(before_rows, after_rows)):
            row_failed = False
            if isinstance(before_item, dict):
                before_text = next(
                    (v for v in before_item.values() if isinstance(v, str)), ""
                )
            else:
                before_text = str(before_item)

            if isinstance(after_item, dict):
                after_text = next(
                    (v for v in after_item.values() if isinstance(v, str)), ""
                )
            else:
                after_text = str(after_item)

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

    def is_deidentified(self, before_rows, after_rows, offset_value: int, pii_data):

        dob_cols = self.pii_config["dob"]
        dob_values = [pii_data[col] for col in dob_cols]
        dob_value = get_best_clean_dob(dob_values)

        normalise_dob = self.extract_dates_from_text(dob_value)[0]["normalized"]
        dob_year = get_year(normalise_dob)
        final_qc_results = {}
        for notes_column in self.columns_names:
            column_qc_result = {"passed_count": 0, "failed_count": 0, "remarks": {}}
            failed_remarks = {
                "exact_match": [],
                "presidio_entities": [],
                "incorrect_masking": [],
                "replace_values_matches": [],
                "date_offset_failed": [],
                "dob_remarks": [],
            }
            for before, after in zip(before_rows, after_rows):
                after_text = after[notes_column]
                before_text = before[notes_column]
                exact_matches = self._exact_match(after_text, pii_data)
                replace_values_matches = self.qc_replace_value(before_rows, after_rows)
                presidio_entities = self._presidio_analyzer(after_text, pii_data)
                before_dates = self.extract_dates_from_text(before_text)
                after_dates = self.extract_dates_from_text(after_text)
                i = 0
                j = 0
                prev_date_indx = 0
                max_index = len(after_text) - 1
                while i < len(before_dates) and j < len(after_dates):
                    # for i in range(min(len(before_dates), len(after_dates))):
                    bd = before_dates[i]["normalized"]
                    ad = after_dates[j]["normalized"]
                    ad_start_index = after_dates[j]["start"]
                    if bd == normalise_dob:
                        dob_failed = False
                        reason = ""
                        if ad == normalise_dob:
                            dob_failed = True
                            reason += "not able to convert dob to year"
                        if (
                            str(dob_year)
                            not in after_text[prev_date_indx:ad_start_index]
                        ):
                            dob_failed = True
                            reason += (
                                "\n year value not found deidentified text for dob"
                            )
                        if dob_failed:
                            failed_remarks["dob_remarks"] = {
                                "failed": dob_failed,
                                "reason": reason,
                            }
                        prev_date_indx = after_dates[j]["end"]
                        i += 1
                        continue

                    if not is_date_offset_valid(bd, ad, offset_value):
                        failed_remarks["date_offset_failed"].append(
                            {"before": bd, "after": ad}
                        )
                    i += 1
                    j += 1
                if len(before_dates) != len(after_dates):
                    failed_remarks["date_offset_failed"].append(
                        {
                            "before_count": len(before_dates),
                            "after_count": len(after_dates),
                            "reason": "Date count mismatch",
                        }
                    )
                if (
                    exact_matches
                    or presidio_entities
                    or failed_remarks["date_offset_failed"]
                ):
                    column_qc_result["failed_count"] += 1
                else:
                    column_qc_result["passed_count"] += 1

                failed_remarks["exact_match"].extend(exact_matches)
                failed_remarks["presidio_entities"].extend(presidio_entities)
                failed_remarks["replace_values_matches"] = replace_values_matches
            failed_remarks["exact_match"] = list(set(failed_remarks["exact_match"]))
            failed_remarks["presidio_entities"] = list(
                set(failed_remarks["presidio_entities"])
            )

            column_qc_result["remarks"] = failed_remarks
            final_qc_results[notes_column] = column_qc_result
        return final_qc_results


before_rows = [
    {
        "notes": "Rohit Chouhan, 1986-04-12, Texas, In facility Texas Patient Jessica Marie Thompson with patient-id: 1001 was admitted on January 1, 1986 and July 22, 2025. She is a 39-year-old woman.",
        "name": "rohit",
    }
]

after_rows = [
    {
        "notes": "((PersonName)), In facility ((FacilityName)) Patient Jessica with patient-id: 10001101011 was admitted on January 31, 1986 and July 22, 2025. She is a 39-year-old woman.",
        "name": "rohit",
    }
]

pii_data = {
    "name": "Jessica Marie Thompson",
    "dob": "1986-04-12",
    "phone": "(415) 763-2184",
    "email": "jessica.thompson86@email.com",
}

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
detector = UnstructuredDetector(columns_names, pii_config=pii_config)
qc_result = detector.is_deidentified(
    before_rows, after_rows, offset_value=offset_value, pii_data=pii_data
)
print(qc_result)

