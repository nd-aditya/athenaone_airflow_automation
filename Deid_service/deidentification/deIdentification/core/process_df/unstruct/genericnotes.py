import pandas as pd
import re
from typing import Dict
from .utils import GENERIC_REGEX_DICT
from core.process_df.rules import RuleBase, BaseDateOffsetRule
from deIdentification.nd_logger import nd_logger
from core.process_df.constants import DATE_PATTERN_NOTES


def remove_inline_flags(pattern: str) -> str:
    """
    Remove inline regex flags (like (?i), (?x), (?ix), etc.) from the start of a pattern.
    These flags cause issues when combining patterns with | operator.
    
    For patterns with (?x) verbose mode, also removes comments and normalizes whitespace.
    """
    original_pattern = pattern
    had_verbose = False
    
    # Match inline flags at the start: (?i), (?x), (?ix), (?i-x), etc.
    # Pattern matches: (? followed by flag letters, optionally with -flag letters, then )
    flag_pattern = re.compile(r'^\(\?([imsxauL]+(?:-[imsxauL]+)?)\)')
    
    # Check if pattern has verbose mode flag
    if re.match(r'^\(\?[imsxauL]*-?x', pattern):
        had_verbose = True
    
    # Remove the flag group
    cleaned = flag_pattern.sub('', pattern)
    
    # If it had verbose mode, remove comments and normalize whitespace
    if had_verbose:
        # Remove comments (everything from # to end of line)
        lines = cleaned.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove comment part (everything after #)
            if '#' in line:
                line = line[:line.index('#')]
            cleaned_lines.append(line)
        cleaned = '\n'.join(cleaned_lines)
        # Normalize whitespace (replace newlines and multiple spaces with single space)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    return cleaned


def mask_address(text: str, compiled_pattern) -> str:
    """Mask an address in the text using named groups."""
    if not isinstance(text, str):
        return text
    
    def _repl(match):
        nd_logger.debug(f"Matched address: {match.group(0)}")
        groups = match.groupdict()
        zip_prefix = groups.get("zip", "")[:3] if groups.get("zip") else ""
        return f"((HouseNumber)) ((StreetName)), ((City)), ((State)) {zip_prefix}".strip()
    
    return compiled_pattern.sub(_repl, text)



class GenericDateShiftRule(BaseDateOffsetRule):
    COMPILED_DATE_PATTERN = re.compile(DATE_PATTERN_NOTES)

    def __init__(self, pii_config):
        super().__init__(pii_config = pii_config,format_as_datetime=False, is_notes=True)

    def get_offset_series(self, df: pd.DataFrame) -> pd.Series:
        # shared logic
        return df.get("_resolved_offset", pd.Series(0, index=df.index))


class GenericNotesRule(RuleBase):
    def get_patterns(self, key: str) -> Dict:
        return GENERIC_REGEX_DICT.get(key, {})
    
    def _apply_replace_value(self, df, column_details) -> pd.Series:
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying static string replacements..."
        )
        text_column = column_details["column_name"]
        df[text_column] = df[text_column].fillna("")
        masked_col = df[text_column].copy()

        replace_rules = self.pii_config.get("replace_value", [])
        if not replace_rules:
            nd_logger.info(
                f"[{self.__class__.__name__}] No static replacement rules found. Skipping replace_value masking."
            )
            return masked_col

        for rule in replace_rules:
            old_value = rule.get("old_value")
            new_value = rule.get("new_value")

            if old_value and new_value:
                pattern = r"(?i)(?<![A-Za-z0-9_]){}(?![A-Za-z0-9_]*@)".format(
                    re.escape(str(old_value))
                )
                masked_col = masked_col.str.replace(pattern, new_value, regex=True)
        
        df[text_column] = masked_col
        return df

    def apply(self, df: pd.DataFrame, column_config: Dict) -> pd.DataFrame:
        col_name = column_config["column_name"]
        nd_logger.info(
            f"[{self.__class__.__name__}] Starting {self.__class__.__name__} for column: {col_name}"
        )
        df = self._apply_replace_value(df, column_config)
        df[col_name] = (df[col_name].astype(str).str.replace(r"\s+", " ", regex=True).str.strip())

        if col_name not in df.columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] Column '{col_name}' not found in DataFrame. Skipping."
            )
            return df

        df[col_name] = df[col_name].astype(str)

        for key, rule in GENERIC_REGEX_DICT.items():
            patterns = rule.get("regex")
            masking_value = rule.get("masking_value", "((MASKED))")
            processing_func = rule.get("processing_func", None)

            if not patterns:
                nd_logger.info(
                    f"[{self.__class__.__name__}] No patterns found for key '{key}'. Skipping."
                )
                continue

            # Normalize to list
            if not isinstance(patterns, list):
                patterns = [patterns]

            nd_logger.info(
                f"[{self.__class__.__name__}] Applying rule: {key}, patterns count: {len(patterns)}"
            )

            if key == "date":
                # Apply GenericDateShiftRule once
                date_rule = GenericDateShiftRule(self.pii_config)
                df = date_rule.apply(df, column_config)


            elif key == "address":
                # Apply address masking with the pattern
                # ADDRESS_REGEX is a single string, not a list
                if isinstance(patterns, str):
                    compiled = re.compile(patterns, re.IGNORECASE | re.VERBOSE)
                    df[col_name] = df[col_name].apply(
                        lambda x: mask_address(x, compiled)
                    )
                else:
                    # Handle as list if it ever becomes one
                    for pattern in patterns:
                        compiled = re.compile(pattern, re.IGNORECASE | re.VERBOSE)
                        df[col_name] = df[col_name].apply(
                            lambda x: mask_address(x, compiled)
                        )
                nd_logger.info(
                    f"[{self.__class__.__name__}] Address rule: pattern completed"
                )

            elif processing_func:
                # Apply custom processing function for each pattern
                for pattern in patterns:
                    compiled = re.compile(pattern, re.IGNORECASE)
                    df[col_name] = df[col_name].apply(
                        lambda x: processing_func(x, compiled, masking_value)
                    )
                    nd_logger.info(
                        f"[{self.__class__.__name__}] Custom rule: pattern={pattern[:30]} completed"
                    )

            else:
                # Optimized path: combine patterns into one for simple replacements
                # Remove inline flags from patterns before combining (flags like (?i), (?x) cause issues)
                cleaned_patterns = [remove_inline_flags(p.strip()) for p in patterns]
                combined_pattern = re.compile(
                    "|".join(f"(?:{p})" for p in cleaned_patterns), re.IGNORECASE
                )
                df[col_name] = df[col_name].str.replace(
                    combined_pattern, masking_value, regex=True
                )
                nd_logger.info(
                    f"[{self.__class__.__name__}] Regex replace rule {key} Done"
                )

        nd_logger.info(
            f"[{self.__class__.__name__}] {self.__class__.__name__} completed."
        )
        return df
