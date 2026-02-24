from locale import D_FMT
import pandas as pd
import re
import itertools
from typing import List
from core.process_df.rules import RuleBase
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select
from core.dbPkg import NDDBHandler
from deIdentification.nd_logger import nd_logger
from dateutil import parser as date_parser
from core.process_df.constants import DATE_PATTERN_NOTES
from core.process_df.exception import RaiseException
from core.process_df.unstruct.genericnotes import GenericNotesRule
from core.process_df.unstruct.xml import deidentify_xml_tags
from core.process_df.unstruct.xml_utils import xml_tag_replacements
from nd_api_v2.models import IncrementalQueue, TableMetadata as TableModel
from nd_api_v2.models.configs import get_pii_config, get_secondary_pii_configs
from core.process_df.unstruct.xml import deidentify_xml_tags
from core.process_df.unstruct.xml_utils import xml_tag_replacements
from rapidfuzz import fuzz, process

Base = declarative_base()


class PIITable:
    def __init__(self):
        self.engine = None
        self.master_session = None

    def _get_db_connection(self, connection_string):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.master_session = Session()

    def close_connection(self):
        if self.master_session:
            self.master_session.close()
        if self.engine:
            self.engine.dispose()

    def _get_table(
        self, table_name, connection_string, nd_patient_ids: list[int]
    ) -> pd.DataFrame:
        self._get_db_connection(connection_string)
        metadata = MetaData()
        pii_table = Table(table_name, metadata, autoload_with=self.engine)

        stmt = select(pii_table).where(pii_table.c.nd_patient_id.in_(nd_patient_ids))
        result = self.master_session.execute(stmt)

        # Convert to DataFrame
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

        self.close_connection()

        return df


class NotesRule(RuleBase):
    def __init__(self, queue_obj: IncrementalQueue, key_phi_columns: tuple, possible_patient_identifier_columns: list, table_obj: TableModel):
        self.table_obj: TableModel = table_obj
        self.pii_config = get_pii_config()
        self.secondary_pii_configs = get_secondary_pii_configs()
        self.pii_data_df = None
        self.secondary_pii_data_dfs = {}
        self.key_phi_columns = key_phi_columns
        self.possible_patient_identifier_columns = possible_patient_identifier_columns
        nd_logger.info(
            f"[{self.__class__.__name__}] Initialized NotesRule with provided DB config."
        )
    
    def _is_fuzzy_matching_enabled(self) -> bool:
        fuzzy_matching_enabled = self.table_obj.run_config.get("enable_fuzzy_matching", False)
        return fuzzy_matching_enabled

    def de_identify_key_phi_columns(
        self, df: pd.DataFrame, column_details: dict
    ) -> pd.DataFrame:
        text_column = column_details["column_name"]

        # Unpack lists of column names from self.key_phi_columns
        encounter_id_cols, patient_id_cols, reference_pid_cols, appointment_id_cols = self.key_phi_columns

        # Take the first column from each list (if any)
        encounter_id_col = encounter_id_cols[0] if encounter_id_cols else None
        patient_id_col = (
            "_resolved_patient_id" if "_resolved_patient_id" in df.columns else None
        )
        reference_pid_col = reference_pid_cols[0] if reference_pid_cols else None
        appointment_id_col = appointment_id_cols[0] if appointment_id_cols else None

        nd_logger.info(
            f"[{self.__class__.__name__}] De-identifying using encounter_id_col={encounter_id_col}, "
            f"patient_id_col={patient_id_col}, reference_pid_col={reference_pid_col}"
        )

        def replace_row(row):
            text = row[text_column]
            replacements = {}

            if encounter_id_col and pd.notnull(row.get(encounter_id_col)):
                original = str(row[encounter_id_col])
                replacement = str(row.get("nd_encounter_id", "((ENCOUNTER_ID))"))
                replacements[re.escape(original)] = replacement

            if appointment_id_col and pd.notnull(row.get(appointment_id_col)):
                original = str(row[appointment_id_col])
                replacement = str(row.get("nd_appointment_id", "((APPOINTMENT_ID))"))
                replacements[re.escape(original)] = replacement

            for col in [patient_id_col, reference_pid_col]:
                if col and pd.notnull(row.get(col)):
                    original = str(row[col])
                    replacement = str(
                        row.get("_resolved_nd_patient_id", "((PATIENT_ID))")
                    )
                    replacements[re.escape(original)] = replacement

            for pattern, repl in replacements.items():
                try:
                    text = re.sub(rf"(?<!\d){pattern}(?!\d)", repl, text)
                except Exception as e:
                    nd_logger.warning(
                        f"[NotesRule] Regex error for pattern {pattern}: {e}"
                    )
            return text

        df[text_column] = df[text_column].fillna("").astype(str)
        df[text_column] = df.apply(replace_row, axis=1)
        nd_logger.info(
            f"[{self.__class__.__name__}] De-identified key PHI values in column '{text_column}'."
        )
        return df

    def _get_pii_data_table(self, nd_patient_ids: list):
        if self.pii_config:
            nd_logger.info(
                f"[{self.__class__.__name__}] Fetching primary PII data for {len(nd_patient_ids)} patients..."
            )
            connection_string = self.pii_config.get("connection_str", None)
            if connection_string:
                pii_table_loader = PIITable()
                self.pii_data_df = pii_table_loader._get_table(
                    "pii_data_table", connection_string, nd_patient_ids
                )
            else:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] connection_str not found in pii_config."
                )
                raise RaiseException("connection_str not found in pii_config")
        else:
            nd_logger.warning(
                f"[{self.__class__.__name__}] pii_config is not defined."
            )
            raise RaiseException("pii_config is not defined")

    def _get_secondary_pii_data_table(self, nd_patient_ids: list):
        if not self.secondary_pii_configs:
            nd_logger.info(
                f"[{self.__class__.__name__}] No secondary PII configs provided."
            )
            # raise RaiseException("secondary PII Configs are not defined")
            return

        connection_string = self.secondary_pii_configs.get("connection_str", None)
        if not connection_string:
            nd_logger.warning(
                f"[{self.__class__.__name__}] connection_str not found in for secondary pii tables."
            )
            # raise RaiseException("secondary PII Configs are not defined")
            return

        nd_logger.info(
            f"[{self.__class__.__name__}] Fetching PII data for {len(nd_patient_ids)} patients..."
        )

        for table_config in self.secondary_pii_configs.get("tables_config", []):
            table_name = table_config.get("table_name")
            if not table_name:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] Skipping entry with missing table_name."
                )
                continue

            pii_table_loader = PIITable()
            self.secondary_pii_data_dfs[table_name] = pii_table_loader._get_table(
                table_name, connection_string, nd_patient_ids
            )

    def deidentify_primary_pii_values(
        self, df: pd.DataFrame, column_details: dict
    ) -> pd.DataFrame:
        text_column = column_details["column_name"]
        df[text_column] = df[text_column].fillna("")

        # Add row number to pii_data_df per patient
        pii_df = self.pii_data_df.copy()
        pii_df["_row_num"] = pii_df.groupby("nd_patient_id").cumcount() + 1
        max_row_num = pii_df["_row_num"].max()
        nd_logger.info(
            f"[{self.__class__.__name__}] Max PII records per patient: {max_row_num}"
        )

        mask_config = self.pii_config.get("mask", {})

        masked_col = df[text_column].copy()
        continue_masking = True

        try:
            max_row_num = int(max_row_num)
            if max_row_num <= 0:
                raise ValueError("Non-positive row number")
        except (TypeError, ValueError):
            nd_logger.warning(
                f"[{self.__class__.__name__}] Records for patients not found or invalid max_row_num. "
                f"[{self.__class__.__name__}] Skipping PII masking, PII DOB masking, PII Combine Masking."
            )
            continue_masking = False

        if continue_masking:
            for row_num in range(1, max_row_num + 1):
                nd_logger.info(
                    f"[{self.__class__.__name__}] Applying PII from row_num {row_num}..."
                )
                pii_batch = pii_df[pii_df["_row_num"] == row_num].drop(
                    columns=["_row_num"]
                )

                df_batch = df.merge(
                    pii_batch,
                    how="left",
                    left_on="_resolved_nd_patient_id",
                    right_on="nd_patient_id",
                )
                df_batch = df_batch.set_index(df.index)

                if self._is_fuzzy_matching_enabled():
                    masked_col = self._apply_mask_batched_fuzzy_matching(df_batch, masked_col, mask_config)
                else:
                    masked_col = self._apply_mask_batched_exact_match(df_batch, masked_col, mask_config)
                masked_col = self._apply_dob(df_batch, masked_col)
                masked_col = self._apply_combine(df_batch, masked_col)

        masked_col = self._apply_regex(masked_col)
        # masked_col = self._apply_replace_value(masked_col)

        df[text_column] = masked_col
        nd_logger.info(
            f"[{self.__class__.__name__}] Finished Primary PII data de-identification for column '{text_column}'."
        )
        return df

    def deidentify_secondary_pii_values(
        self, df: pd.DataFrame, column_details: dict
    ) -> pd.DataFrame:
        text_column = column_details["column_name"]
        df[text_column] = df[text_column].fillna("")
        masked_col = df[text_column].copy()

        if not self.secondary_pii_data_dfs:
            nd_logger.info(
                f"[{self.__class__.__name__}] No secondary PII data found to apply masking."
            )
            return df

        if not self.secondary_pii_configs:
            nd_logger.info(f"[{self.__class__.__name__}] No secondary PII configs provided.")
            #raise RaiseException("secondary PII Configs are not defined")
            return

        for table_config in self.secondary_pii_configs['tables_config']:
            table_name = table_config.get("table_name")
            if not table_name:
                nd_logger.warning(f"[{self.__class__.__name__}] Skipping entry with missing table_name.")
                continue

            pii_df = self.secondary_pii_data_dfs.get(table_name)
            
            if pii_df.empty:
                nd_logger.info(
                    f"[{self.__class__.__name__}] Skipping secondary table '{table_name}' as it's empty."
                )
                continue

            mask_config = table_config.get("config", {})

            # Add row number per patient
            pii_df = pii_df.copy()
            pii_df["_row_num"] = pii_df.groupby("nd_patient_id").cumcount() + 1
            max_row_num = pii_df["_row_num"].max()
            nd_logger.info(
                f"[{self.__class__.__name__}] [{table_name}] Max PII records per patient: {max_row_num}"
            )

            for row_num in range(1, max_row_num + 1):
                nd_logger.info(
                    f"[{self.__class__.__name__}] [{table_name}] Applying PII from row_num {row_num}..."
                )
                pii_batch = pii_df[pii_df["_row_num"] == row_num].drop(
                    columns=["_row_num"]
                )

                df_batch = df.merge(
                    pii_batch,
                    how="left",
                    left_on="_resolved_nd_patient_id",
                    right_on="nd_patient_id",
                )
                df_batch = df_batch.set_index(df.index)

                masked_col = self._apply_mask_batched_exact_match(df_batch, masked_col, mask_config)

        df[text_column] = masked_col
        nd_logger.info(
            f"[{self.__class__.__name__}] Finished applying secondary PII masking for column '{text_column}'."
        )
        return df
    
    def _apply_mask_batched_exact_match(self, df_batch: pd.DataFrame, masked_col: pd.Series, mask_config:dict) -> pd.Series:
        nd_logger.info(f"[{self.__class__.__name__}] Applying exact match MASKing using values from external PII source table...")

        if not mask_config:
            nd_logger.info(f"[{self.__class__.__name__}] No MASK config found. Skipping exact match masking.")
            return masked_col

        # Step 1: Prepare a DataFrame with only relevant PII columns
        pii_columns = [col for col in mask_config if col in df_batch.columns]
        if not pii_columns:
            nd_logger.warning(f"[{self.__class__.__name__}] No matching PII columns found in df_batch for masking. Skipping.")
            return masked_col
        
        nd_logger.debug(f"[{self.__class__.__name__}] Masking PII columns: {pii_columns}")
        pii_df = df_batch[pii_columns].astype(str).fillna("")

        # Step 2: Build a mapping of PII value => mask_value per row
        # This gives a Series of dicts: { "John": "((PATIENT_NAME))", "Boston": "((CITY))", ... }
        pii_replacements = []
        for i, row in pii_df.iterrows():
            replacement_map = {}
            for col in pii_columns:
                val = row[col].strip()
                if val:  # non-empty
                    if len(val) < 3:
                        pattern = rf"(?i)(?:['\"]?\b{re.escape(val)}\b['\"]?)"
                    else:
                        pattern = r"(?i)(?<![A-Za-z0-9_])(['\"/])?{}(['\"])?(?![A-Za-z0-9_]*@)".format(
                            re.escape(val)
                        )

                    replacement_map[pattern] = mask_config[col]["mask_value"]
            pii_replacements.append(replacement_map)

        # Step 3: Apply str.replace for each row using that row’s replacement map
        def replace_row(text, replacements):
            if not replacements:
                return text
            return pd.Series(text).replace(replacements, regex=True).iloc[0]

        masked_col = [
            replace_row(text, pii_replacements[i])
            for i, text in enumerate(masked_col)
        ]
        nd_logger.info(f"[{self.__class__.__name__}] Finished exact match MASKing.")
        return pd.Series(masked_col, index=df_batch.index)

    def _apply_mask_batched_fuzzy_matching(
        self, df_batch: pd.DataFrame, masked_col: pd.Series, mask_config: dict
    ) -> pd.Series:
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying fuzzy match MASKing using values from external PII source table..."
        )
        mask_config = self.pii_config.get("mask", {})

        if not mask_config:
            nd_logger.info(
                f"[{self.__class__.__name__}] No MASK config found. Skipping fuzzy match masking."
            )
            return masked_col

        # Step 1: Prepare a DataFrame with only relevant PII columns
        pii_columns = [col for col in mask_config if col in df_batch.columns]
        if not pii_columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No matching PII columns found in df_batch for masking. Skipping."
            )
            return masked_col

        nd_logger.debug(
            f"[{self.__class__.__name__}] Masking PII columns: {pii_columns}"
        )
        pii_df = df_batch[pii_columns].astype(str).fillna("")

        # Step 2: Build a mapping of PII values for fuzzy matching per row
        pii_values_per_row = []

        for i, row in pii_df.iterrows():
            row_pii_values = {}
            for col in pii_columns:
                val = row[col].strip()
                # Filter out None, null, empty strings, and values with length < 3
                if val and val != "None" and val != "null" and len(val) >= 3:
                    row_pii_values[col] = val
            pii_values_per_row.append(row_pii_values)

        # Step 3: Apply fuzzy matching for each row
        def fuzzy_replace_row(text, pii_values, mask_config):
            if not isinstance(text, str) or not pii_values:
                return text
            
            result_text = text
            for col, pii_value in pii_values.items():
                mask_value = mask_config[col]["mask_value"]
                
                # Normalize the PII value for better matching
                normalized_pii = self._normalize_text_for_fuzzy_matching(pii_value)
                
                # Find fuzzy matches in the text
                fuzzy_matches = self._find_fuzzy_matches_in_text(text, normalized_pii)
                
                # Replace each fuzzy match with the masking value
                for match_text in fuzzy_matches:
                    # Use word boundaries to avoid partial word replacements
                    pattern = re.compile(r'\b' + re.escape(match_text) + r'\b', re.IGNORECASE)
                    result_text = pattern.sub(mask_value, result_text)
            
            return result_text

        masked_col = [
            fuzzy_replace_row(text, pii_values_per_row[i], mask_config) 
            for i, text in enumerate(masked_col)
        ]
        nd_logger.info(f"[{self.__class__.__name__}] Finished fuzzy match MASKing.")
        return pd.Series(masked_col, index=df_batch.index)

    def _normalize_text_for_fuzzy_matching(self, text: str) -> str:
        """
        Normalize text for better fuzzy matching by handling special characters
        and common variations.
        """
        if not isinstance(text, str):
            return str(text)
        
        # Convert to lowercase for case-insensitive matching
        normalized = text.lower().strip()
        
        # Handle common apostrophe variations
        normalized = re.sub(r"[''`]", "'", normalized)
        
        # Handle common hyphen variations
        normalized = re.sub(r"[-–—]", "-", normalized)
        
        # Remove extra whitespace
        normalized = re.sub(r"\s+", " ", normalized)
        
        return normalized

    def _find_fuzzy_matches_in_text(self, text: str, pii_value: str, threshold: int = 80) -> List[str]:
        """
        Find fuzzy matches of PII value in text using rapidfuzz.
        Returns a list of actual text snippets that match the PII value.
        """
        if not isinstance(text, str) or not isinstance(pii_value, str):
            return []
        
        # Split text into words for better matching
        words = re.findall(r'\b\w+\b', text.lower())
        
        # Also check for multi-word phrases
        text_lower = text.lower()
        matches = []
        
        # Check for exact word matches first
        if pii_value.lower() in text_lower:
            # Find all occurrences of the exact match
            start = 0
            while True:
                pos = text_lower.find(pii_value.lower(), start)
                if pos == -1:
                    break
                # Extract the actual text (preserving original case)
                actual_text = text[pos:pos + len(pii_value)]
                matches.append(actual_text)
                start = pos + 1
        
        # Check for fuzzy matches in individual words
        for word in words:
            if len(word) >= 3:  # Only check words with length >= 3
                similarity = fuzz.ratio(pii_value.lower(), word)
                if similarity >= threshold:
                    # Find the actual word in the original text
                    word_pattern = re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE)
                    for match in word_pattern.finditer(text):
                        matches.append(match.group())
        
        # Check for fuzzy matches in multi-word phrases
        # Split text into overlapping n-grams
        words_list = re.findall(r'\b\w+\b', text_lower)
        for n in range(2, min(len(words_list) + 1, 4)):  # Check 2-3 word phrases
            for i in range(len(words_list) - n + 1):
                phrase = ' '.join(words_list[i:i+n])
                if len(phrase) >= 3:
                    similarity = fuzz.ratio(pii_value.lower(), phrase)
                    if similarity >= threshold:
                        # Find the actual phrase in the original text
                        phrase_pattern = re.compile(r'\b' + re.escape(phrase) + r'\b', re.IGNORECASE)
                        for match in phrase_pattern.finditer(text):
                            matches.append(match.group())
        
        # Remove duplicates while preserving order
        seen = set()
        unique_matches = []
        for match in matches:
            if match.lower() not in seen:
                seen.add(match.lower())
                unique_matches.append(match)
        
        return unique_matches

    def _apply_dob(self, df_batch: pd.DataFrame, masked_col: pd.Series) -> pd.Series:
        nd_logger.info(f"[{self.__class__.__name__}] Applying DOB masking...")

        dob_config = self.pii_config.get("dob", {})
        if not dob_config:
            nd_logger.info(
                f"[{self.__class__.__name__}] No DOB config found. Skipping DOB masking."
            )
            return masked_col

        dob_columns = [col for col in dob_config if col in df_batch.columns]
        if not dob_columns:
            nd_logger.warning(
                f"[{self.__class__.__name__}] No DOB columns found in df_batch."
            )
            return masked_col

        date_pattern = re.compile(DATE_PATTERN_NOTES)
        dob_replacements_list = []

        for _, row in df_batch.iterrows():
            row_map = {}
            for col in dob_columns:
                val = row[col]
                if pd.notnull(val) and str(val).strip():
                    try:
                        parsed_dob = date_parser.parse(str(val), fuzzy=True).date()
                        row_map[parsed_dob] = str(
                            parsed_dob.year
                        )  # dob_config[col]["mask_value"]
                    except Exception as e:
                        nd_logger.debug(
                            f"[{self.__class__.__name__}] Could not parse DOB value in column '{col}': '{val}'. Error: {e}"
                        )
            dob_replacements_list.append(row_map)

        def replace_dates_in_text(text: str, replacements: dict) -> str:
            if not isinstance(text, str) or not replacements:
                return text

            def replacer(match):
                date_str = match.group(0)
                try:
                    parsed_date = date_parser.parse(date_str, fuzzy=True).date()
                    return replacements.get(parsed_date, date_str)
                except Exception:
                    return date_str

            return date_pattern.sub(replacer, text)

        masked_col = [
            replace_dates_in_text(text, dob_replacements_list[i])
            for i, text in enumerate(masked_col)
        ]
        nd_logger.info(f"[{self.__class__.__name__}] Finished DOB masking.")
        return pd.Series(masked_col, index=df_batch.index)

    def _build_rowwise_patterns(self, df_batch: pd.DataFrame) -> dict:
        combine_config = self.pii_config.get("combine", {})
        if not combine_config:
            nd_logger.info(
                f"[{self.__class__.__name__}] No combine config found. Skipping combine masking."
            )
            return {}

        pattern_map = {}

        for rule_name, rule in combine_config.items():
            cols = rule.get("combine", [])
            mask_value = rule.get("mask_value", "")

            nd_logger.debug(
                f"[{self.__class__.__name__}] [Combine:{rule_name}] Columns to combine: {cols}"
            )
            nd_logger.debug(
                f"[{self.__class__.__name__}] [Combine:{rule_name}] Masking value: {mask_value}"
            )

            cols = [col for col in cols if col in df_batch.columns]
            if not cols:
                nd_logger.warning(
                    f"[{self.__class__.__name__}] [Combine:{rule_name}] Skipping — none of the columns found in DataFrame."
                )
                continue

            def generate_patterns(row) -> List[str]:
                values = [
                    str(row[col]).strip()
                    for col in cols
                    if pd.notnull(row[col]) and str(row[col]).strip()
                ]
                combinations = set()
                for r in range(1, len(values) + 1):
                    for perm in itertools.permutations(values, r):
                        combined = "".join(perm).strip().lower()
                        if len(combined) > 2:
                            combinations.add(combined)
                return list(combinations)

            patterns_series = df_batch.apply(generate_patterns, axis=1)
            pattern_map[rule_name] = {
                "patterns_series": patterns_series,
                "mask_value": mask_value,
            }

            nd_logger.debug(
                f"[{self.__class__.__name__}] [Combine:{rule_name}] Sample row patterns: {patterns_series.head().tolist()}"
            )

        return pattern_map

    def _apply_combine(
        self, df_batch: pd.DataFrame, masked_col: pd.Series
    ) -> pd.Series:
        nd_logger.info(
            f"[{self.__class__.__name__}] Applying row-specific combined PII masking (vectorized with permutations)..."
        )
        pattern_map = self._build_rowwise_patterns(df_batch)

        if not pattern_map:
            nd_logger.info(
                f"[{self.__class__.__name__}] No combine patterns built. Skipping combine masking."
            )
            return masked_col

        for rule_name, info in pattern_map.items():
            patterns_series = info["patterns_series"]
            mask_value = info["mask_value"]

            def mask_row(note_text, patterns: List[str]) -> str:
                if not patterns:
                    return note_text
                try:
                    sorted_patterns = sorted(patterns, key=len, reverse=True)
                    #pattern = re.compile("|".join(re.escape(p) for p in sorted_patterns), re.IGNORECASE)
                    pattern = re.compile("|".join(rf"\b{re.escape(p)}\b" for p in sorted_patterns), re.IGNORECASE)
                    if not isinstance(note_text, str):
                        return note_text
                    return pattern.sub(mask_value, note_text)
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] [Combine:{rule_name}] Regex failed for row: {e}"
                    )
                    return note_text

            masked_col = masked_col.combine(patterns_series, mask_row)

            nd_logger.debug(
                f"[{self.__class__.__name__}] [Combine:{rule_name}] Completed masking."
            )

        return masked_col

    def _apply_regex(self, masked_col: pd.Series) -> pd.Series:
        nd_logger.info(f"[{self.__class__.__name__}] Applying regex-based masking...")
        regex_config = self.pii_config.get("regex", {})
        if not regex_config:
            nd_logger.info(
                f"[{self.__class__.__name__}] No regex patterns found in config. Skipping PII regex-based masking."
            )
            return masked_col

        for key, conf in regex_config.items():
            patterns = (
                conf["regex"] if isinstance(conf["regex"], list) else [conf["regex"]]
            )
            mask_value = conf["mask_value"]

            for pat in patterns:
                try:
                    compiled = re.compile(pat, re.IGNORECASE)
                    masked_col = masked_col.str.replace(
                        compiled, mask_value, regex=True
                    )
                except Exception as e:
                    nd_logger.warning(
                        f"[{self.__class__.__name__}] Regex compile failed for pattern '{pat}': {e}"
                    )

        return masked_col


    def apply(self, df: pd.DataFrame, column_details: dict) -> pd.DataFrame:
        """
        Apply the NotesRule to de-identify notes using patient-specific PII data.

        :param df: Input DataFrame containing a 'nd_patient_id' and target text column.
        :param column_details: Dict containing 'column' key which points to column to mask.
        :return: DataFrame with additional column containing masked results.
        """
        nd_logger.info(f"[{self.__class__.__name__}] Starting de-identification for {len(df)} rows...")

        text_column = column_details.get("column_name")
        if text_column and text_column in df.columns:
            df[text_column] = (df[text_column]
                    .apply(lambda x: "" if pd.isna(x) or x is None else str(x))  # handles NaN + None
                    .str.replace(r"\s+", " ", regex=True)
                    .str.replace("^", " ", regex=False)      # replace ^ with space
                    .str.strip())

        def apply_xml_masking(text):
            return deidentify_xml_tags(text, xml_tag_replacements)

        df[text_column] = df[text_column].apply(apply_xml_masking)
        nd_logger.info(f"[{self.__class__.__name__}] Applied XML tag-based masking for column '{text_column}'.")

        if "_resolved_nd_patient_id" in df.columns:
            patient_ids = df["_resolved_nd_patient_id"].dropna().unique().tolist()

            #df = self.de_identify_key_phi_columns(df, column_details)

            if self.pii_data_df is None:
                self._get_pii_data_table(patient_ids)

            if not self.secondary_pii_data_dfs:
                self._get_secondary_pii_data_table(patient_ids)

            df = self.deidentify_primary_pii_values(df, column_details)

            df = self.deidentify_secondary_pii_values(df, column_details)

        df = GenericNotesRule(self.pii_config).apply(df, column_details)

        return df