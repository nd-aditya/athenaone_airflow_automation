"""
PHI Validation Tools
Contains specialized tools for validating different types of PHI elements
"""
import traceback
import logging
import re
import threading
from typing import List, Dict, Any, Optional, TypedDict
from datetime import datetime
import pandas as pd
from abc import ABC, abstractmethod

SAMPLE_SIZE = 50
THRESHOLD_MATCH = 0.7

# Global cache to store mapping table values per process lifetime
_MAPPING_CACHE: Dict[str, Any] = {
    "loaded": False,
    "patient": {},  # column_name -> set(values)
    "encounter": None  # column_name (encounter id) -> set(values) or just set
}

# Lock for thread-safe cache initialization
_CACHE_LOCK = threading.Lock()
_CACHE_INITIALIZED = False

def build_mapping_cache1(config: Dict[str, Any], mapping_db_manager) -> Dict[str, Any]:
    tmp_cache = {}
    tmp_cache['loaded'] = True
    tmp_cache['patient'] = {
        "patient_id": [10, 100, 1000, 10000]
    }
    tmp_cache['encounter'] = [1, 100, 1000, 1000]
    return tmp_cache



def build_mapping_cache(config: Dict[str, Any], mapping_db_manager) -> Dict[str, Any]:
    """Build a mapping cache dictionary from DB (does not mutate global cache)."""
    tmp_cache: Dict[str, Any] = {"loaded": True, "patient": {}, "encounter": None}
    logger = logging.getLogger(__name__)
    logger.info("Starting to build mapping cache from database...")
    
    master_cfg = config.get('mapping_table', {})
    patient_table = master_cfg.get('patient_mapping_table_name')
    patient_cols = master_cfg.get('patient_identifier_columns') or []
    encounter_table = master_cfg.get('encounter_mapping_table_name')
    encounter_id_col = master_cfg.get('encounter_id_column')

    # Load patient identifiers
    if patient_table and patient_cols:
        for pid_col in patient_cols:
            try:
                values = mapping_db_manager.get_all_valid_rows_cache(patient_table, pid_col)
                tmp_cache["patient"][pid_col] = set(str(v).strip() for v in values if v is not None)
                logger.info(f"Built mapping cache for patient column '{pid_col}' with {len(tmp_cache['patient'][pid_col])} unique values")
            except Exception as e:
                logger.warning(f"Failed building mapping cache for {patient_table}.{pid_col}: {str(e)}")

    # Load encounter identifiers
    if encounter_table and encounter_id_col:
        try:
            values = mapping_db_manager.get_all_valid_rows(encounter_table, encounter_id_col)
            tmp_cache["encounter"] = set(str(v).strip() for v in values if v is not None)
            logger.info(f"Built mapping cache for encounter id '{encounter_id_col}' with {len(tmp_cache['encounter']) if tmp_cache['encounter'] else 0} unique values")
        except Exception as e:
            logger.warning(f"Failed building mapping cache for {encounter_table}.{encounter_id_col}: {str(e)}")

    logger.info("Mapping cache building completed successfully")
    return tmp_cache

def init_mapping_cache(preloaded_cache: Dict[str, Any]) -> None:
    """Initializer to set the global mapping cache (used by worker processes)."""
    global _MAPPING_CACHE, _CACHE_INITIALIZED
    _MAPPING_CACHE = preloaded_cache or {"loaded": True, "patient": {}, "encounter": None}
    _CACHE_INITIALIZED = True

def _load_mapping_cache(config: Dict[str, Any], mapping_db_manager) -> None:
    """Load patient and encounter mapping tables into memory once per process if not already loaded."""
    global _MAPPING_CACHE, _CACHE_INITIALIZED, _CACHE_LOCK
    
    # Double-checked locking pattern for thread safety
    if _CACHE_INITIALIZED and _MAPPING_CACHE.get("loaded"):
        return
        
    with _CACHE_LOCK:
        # Check again inside the lock
        if _CACHE_INITIALIZED and _MAPPING_CACHE.get("loaded"):
            return
            
        logger = logging.getLogger(__name__)
        try:
            logger.info("Building mapping cache - this should happen only once per process")
            built = build_mapping_cache(config, mapping_db_manager)
            init_mapping_cache(built)
            _CACHE_INITIALIZED = True
            logger.info("Mapping cache successfully initialized")
        except Exception as e:
            logger.error(f"Error initializing mapping cache: {str(e)}")
            _CACHE_INITIALIZED = False

# Expose an initializer function for multiprocessing.Pool
def init_worker_mapping_cache(preloaded_cache: Dict[str, Any]) -> None:
    """Initialize worker process with prebuilt mapping cache."""
    logger = logging.getLogger(__name__)
    logger.info(f"Worker process initializing with prebuilt cache (patient keys: {list(preloaded_cache.get('patient', {}).keys())})")
    init_mapping_cache(preloaded_cache)

def is_mapping_cache_loaded() -> bool:
    """Check if mapping cache is already loaded."""
    global _CACHE_INITIALIZED, _MAPPING_CACHE
    return _CACHE_INITIALIZED and _MAPPING_CACHE.get("loaded", False)


class LLMResult(TypedDict):
    rule_detected: str

class BasePHIValidator(ABC):
    """Base class for PHI validators"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        """
        Validate if the column contains the specific PHI type
        
        Args:
            db_name: Database name
            table_name: Table name
            column_name: Column name
            
        Returns:
            Boolean indicating if column contains the PHI type
        """
        pass


ALL_PATIENT_IDENTIFIER_PATTERNS = {
    "patient_id": [r'.*patient.*id.*', r'.*mrn.*', r'.*user.*id.*',r'.*uid.*'],
    "chart_id": [r'.*chart.*id.*'],
    "enterprise_id": [r'.*enterprise.*id.*'],
    "patient_profile_id": [r'.*patient.*profile.*id.*'],
}


class PatientIDValidator(BasePHIValidator):
    """Validator for Patient ID columns"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_db_manager = mapping_database_manager
        self.master_mapping_config = config.get('mapping_table', {})
        self.patient_identifier_columns = config.get('mapping_table', {}).get("patient_identifier_columns", [])
    
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str,str]:
        """
        Validate if column is a patient ID by checking:
        1. Column name patterns
        2. Data format patterns
        3. Cross-reference with master mapping table
        
        Args:
            db_name: Database name
            table_name: Table name  
            column_name: Column name
            
        Returns:
            Boolean indicating if column is a patient ID
        -- PATIENT_PATIENTID, PATIENT_CHARTID, PATIENT_PROFILEID

        Steps:
            0. vartype (must be numeric)
            1. pattern matching on column name
            2. check this values exists in patient-mapping table
            3. generate output with flag -> (if values are null, regex not found) -> threshold match :: 70%
        """
        try:
            rule_detected = None
            PipelineRemarks = None
            is_phi = False
            self.logger.info(f"Validating Patient ID: {table_name}.{column_name}")
            
            if not self.db_manager.check_column_type(table_name, column_name, 'numeric'):
                self.logger.debug(f"Column {column_name} is not numeric")
                PipelineRemarks = "NOT_NUMERIC"
                return False, PipelineRemarks, rule_detected
            else:
                PipelineRemarks = "NUMERIC"
            pattern_map = {}
            for _pid in self.patient_identifier_columns:
                pattern_map[_pid] = ALL_PATIENT_IDENTIFIER_PATTERNS[_pid]
            
            column_lower = column_name.lower()
            matched_pattern_type = None
            name_match = False
            for pattern_type, patterns in pattern_map.items():
                if any(re.match(pattern, column_lower) for pattern in patterns):
                    matched_pattern_type = pattern_type
                    name_match = True
                    break

            if not name_match:
                self.logger.debug(f"Column name {column_name} doesn't match patient ID patterns")
                PipelineRemarks += "+ PATTERN_NOT_MATCHED"
            else:
                PipelineRemarks += f"+ PATTERN_MATCHED({matched_pattern_type})"

            # Get sample values to analyze format
            sample_values = self.db_manager.get_all_valid_rows(table_name, column_name)

            if not sample_values:
                self.logger.warning(f"No sample values found for {table_name}.{column_name}")
                PipelineRemarks += "+ ALL_VALUES_ARE_NONE"
                return False, PipelineRemarks, rule_detected
            
            mapping_table = self.master_mapping_config.get('patient_mapping_table_name')
            all_pid_cols = self.master_mapping_config.get('patient_identifier_columns')
            
            if not mapping_table or not all_pid_cols:
                self.logger.warning(f"Missing patient mapping configuration: table={mapping_table}, columns={all_pid_cols}")
                PipelineRemarks += "+ MAPPING_CONFIG_MISSING"
                return is_phi, PipelineRemarks, rule_detected
            
            # Use cached mapping values
            threshold_map = {}
            for _pid_col in all_pid_cols:
                if matched_pattern_type is not None and matched_pattern_type != _pid_col:
                    continue
                cached_set = _MAPPING_CACHE.get('patient', {}).get(_pid_col)
                if cached_set:
                    self.logger.debug(f"Using cached patient data for {_pid_col} with {len(cached_set)} values")
                    similarity_score = self._calculate_value_similarity(sample_values, list(cached_set))
                    threshold_map[_pid_col] = similarity_score
                else:
                    self.logger.warning(f"No cached data found for patient column {_pid_col}")

            best_pid_col = max(threshold_map, key=threshold_map.get)
            similarity_score = threshold_map[best_pid_col]
            
            if similarity_score > THRESHOLD_MATCH:
                self.logger.info(f"Patient ID validated with high confidence: {table_name}.{column_name}")
                PipelineRemarks += f"+ THRESHOLD_({similarity_score})_CROSSED_FOR_{best_pid_col}"
                if name_match:
                    rule_detected = f"PATIENT_{best_pid_col.replace('_', '').upper()}"
                else:
                    rule_detected = f"PATIENT_PATIENTID"
                is_phi = True
            else:
                PipelineRemarks += f"+ THRESHOLD_({similarity_score})_NOT_CROSSED"
                is_phi = False
                rule_detected = None
            
            self.logger.info(f"Patient ID validated: {table_name}.{column_name}")
            return is_phi, PipelineRemarks, rule_detected
            
        except Exception as e:
            traceback.print_exc()
            self.logger.error(f"Error validating patient ID {table_name}.{column_name}: {str(e)}")
            return False, PipelineRemarks, None
    
    def _calculate_value_similarity(self, source_values: List[Any], mapping_values: List[Any]) -> float:
        """
        Calculate similarity as the percentage of source values present in mapping values,
        with flexible matching for numeric values (e.g., 144.00 == 144).
        """
        try:
            if not source_values or not mapping_values:
                return 0.0

            def normalize(val):
                if val is None:
                    return None
                s = str(val).strip()
                # Try to convert to float, then to int if possible
                try:
                    f = float(s)
                    if f.is_integer():
                        return str(int(f))
                    else:
                        return str(f)
                except Exception:
                    return s

            mapping_set = set(normalize(v) for v in mapping_values if v is not None)
            source_list = [normalize(v) for v in source_values if v is not None]

            if not source_list or not mapping_set:
                return 0.0

            match_count = sum(1 for v in source_list if v in mapping_set)
            return match_count / len(source_list) if source_list else 0.0

        except Exception:
            return 0.0


class EncounterIDValidator(BasePHIValidator):
    """Validator for Encounter ID columns"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_db_manager = mapping_database_manager
        self.master_mapping_config = config.get('mapping_table', {})
    
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        """
        Validate if column is an encounter ID
        
        Args:
            db_name: Database name
            table_name: Table name
            column_name: Column name
            
        Returns:
            Boolean indicating if column is an encounter ID
        
        Steps:
            0. vartype (must be numeric)
            1. pattern matching on column name
            2. check this values exists in encounter-mapping table
            3. generate output with flag -> (if values are null, regex not found) -> threshold match :: 70%
        """
        try:
            rule_detected = None
            PipelineRemarks = None
            is_phi = False
            self.logger.info(f"Validating Encounter ID: {table_name}.{column_name}")
            
            if not self.db_manager.check_column_type(table_name, column_name, 'numeric'):
                self.logger.debug(f"Column {column_name} is not numeric")
                PipelineRemarks = "NOT_NUMERIC"
                return is_phi, PipelineRemarks, rule_detected
            else:
                PipelineRemarks = "NUMERIC"
            
            
            # Check column name patterns
            encounter_id_patterns = [
                r'.*encounter.*id.*', r'.*eid.*', r'.*enc.*id.*',
                r'.*clinical.*encounter.*', r'.*visit.*id.*'
            ]
            
            column_lower = column_name.lower()
            name_match = any(re.match(pattern, column_lower) for pattern in encounter_id_patterns)
            
            if not name_match:
                self.logger.debug(f"Column name {column_name} doesn't match encounter ID patterns")
                PipelineRemarks += "+ PATTERN_NOT_MATCHED"
            else:
                PipelineRemarks += f"+ PATTERN_MATCHED"
            
            sample_values = self.db_manager.get_all_valid_rows(table_name, column_name)
            
            if not sample_values:
                self.logger.warning(f"No sample values found for {table_name}.{column_name}")
                PipelineRemarks += "+ ALL_VALUES_ARE_NONE"
                return is_phi, PipelineRemarks, rule_detected
            
            mapping_table = self.master_mapping_config.get('encounter_mapping_table_name')
            enc_id = self.master_mapping_config.get('encounter_id_column')
            
            if not mapping_table or not enc_id:
                self.logger.warning(f"Missing mapping table configuration: table={mapping_table}, column={enc_id}")
                PipelineRemarks += "+ MAPPING_CONFIG_MISSING"
                return is_phi, PipelineRemarks, rule_detected
            
            # Use cached encounter id set
            cached_set = _MAPPING_CACHE.get('encounter')
            if cached_set:
                self.logger.debug(f"Using cached encounter data with {len(cached_set)} values")
                similarity_score = self._calculate_value_similarity(sample_values, list(cached_set))
            else:
                self.logger.warning("No cached encounter data found")
                similarity_score = 0.0

            if similarity_score > THRESHOLD_MATCH:
                self.logger.info(f"Patient ID validated with high confidence: {table_name}.{column_name}")
                PipelineRemarks += f"+ THRESHOLD_({similarity_score})_CROSSED"
                is_phi = True
                rule_detected = "ENCOUNTER_ID"
            else:
                PipelineRemarks += f"+ THRESHOLD_({similarity_score})_NOT_CROSSED"
                is_phi = False
            self.logger.info(f"Encounter ID validated: {table_name}.{column_name}")
            return is_phi, PipelineRemarks, rule_detected
            
        except Exception as e:
            self.logger.error(f"Error validating encounter ID {table_name}.{column_name}: {str(e)}")
            return False, PipelineRemarks, rule_detected
        
    def _calculate_value_similarity(self, source_values: List[Any], mapping_values: List[Any]) -> float:
        """
        Calculate similarity as the percentage of source values present in mapping values,
        with flexible matching for numeric values (e.g., 144.00 == 144).
        """
        try:
            if not source_values or not mapping_values:
                return 0.0

            def normalize(val):
                if val is None:
                    return None
                s = str(val).strip()
                # Try to convert to float, then to int if possible
                try:
                    f = float(s)
                    if f.is_integer():
                        return str(int(f))
                    else:
                        return str(f)
                except Exception:
                    return s

            mapping_set = set(normalize(v) for v in mapping_values if v is not None)
            source_list = [normalize(v) for v in source_values if v is not None]

            if not source_list or not mapping_set:
                return 0.0

            match_count = sum(1 for v in source_list if v in mapping_set)
            return match_count / len(source_list) if source_list else 0.0

        except Exception:
            return 0.0


class DOBValidator(BasePHIValidator):
    """Validator for Date of Birth columns"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_database_manager = mapping_database_manager
    
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        """
        Validate if column contains date of birth information
        
        Args:
            db_name: Database name
            table_name: Table name
            column_name: Column name
            
        Returns:
            Boolean indicating if column contains DOB
        
        Steps
            1. identify if it is date type 
                a. vartype check (date/datetime) -> mark as date 
                b. if datetime is in varchar(100) -> exact date pattern -> mark as date
            2. identify whether it is doB or normal date
                a. noramlly dob pattern matching work for DOB rule, if not match then normal date
        """
        try:
            rule_detected = None
            PipelineRemarks = None
            is_phi = False
            self.logger.info(f"Validating DOB: {table_name}.{column_name}")
            
            if self.db_manager.check_column_type(table_name, column_name, 'date'):
                PipelineRemarks = "DATETYPE"
            else:
                self.logger.debug(f"Column {column_name} is not date")
                PipelineRemarks = "NOT DATETYPE"

            
            # Check column name patterns
            dob_patterns = [
                r'.*dob.*', r'.*birth.*date.*', r'.*date.*birth.*', r'.*born.*date*', r'.*date.*born*'
            ]
            
            column_lower = column_name.lower()
            name_match = any(re.match(pattern, column_lower) for pattern in dob_patterns)
        
            
            is_phi = True if name_match else False
            rule_detected = "DOB" if name_match else None
            
            self.logger.debug(f"DOB validation completed: {table_name}.{column_name}")
            
            return is_phi, PipelineRemarks, rule_detected
            
        except Exception as e:
            self.logger.error(f"Error validating DOB {table_name}.{column_name}: {str(e)}")
            return False, PipelineRemarks, rule_detected
        
class DateValidator(BasePHIValidator):
    """Validator for date and timestamp columns (excluding DOB)"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_database_manager = mapping_database_manager
    
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        """
        Validate if column contains date/timestamp information
        
        Args:
            db_name: Database name
            table_name: Table name
            column_name: Column name
            
        Returns:
            Boolean indicating if column contains dates
        
        Steps
            1. identify if it is date type 
                a. vartype check (date/datetime) -> mark as date 
                b. if datetime is in varchar(100) -> exact date pattern -> mark as date
        """
        rule_detected = None
        PipelineRemarks = llm_result
        is_phi = False
        try:
            self.logger.info(f"Validating Date: {table_name}.{column_name}")
            if self.db_manager.check_column_type(table_name, column_name, 'date'):
                PipelineRemarks += "+DATETYPE"
            elif self.db_manager.check_column_type(table_name, column_name, 'time'):
                PipelineRemarks += "+TIMETYPE"
                return False, PipelineRemarks, None
            else:
                self.logger.debug(f"Column {column_name} is not date")
                PipelineRemarks += "+NOT_DATETYPE"
                if "+LLM_MARKED_PHI_NO_BUT_CHECKING_FOR_DATE" in PipelineRemarks:
                    return False, PipelineRemarks, None

            
            
            # Exclude DOB columns
            if any(pattern in column_name.lower() for pattern in ['dob', 'birth']):
                self.logger.debug(f"Column {column_name} identified as DOB, not general date")
                PipelineRemarks += "+ CONTAIN_DOB_IN_COLUMN_NAME"
                return True, PipelineRemarks, "DOB"
            
            if "NOT_DATETYPE" not in PipelineRemarks:
                return True, PipelineRemarks, "DATE_OFFSET"
            
            sample_values = self.db_manager.get_all_valid_rows(table_name, column_name)
            
            if not sample_values:
                self.logger.warning(f"No sample values found for {table_name}.{column_name}")
                PipelineRemarks += "+ ALL_VALUES_ARE_NONE"
                return is_phi, PipelineRemarks, rule_detected
            
            valid_date_count = 0
            for value in sample_values:
                if value is not None and self._is_valid_date(value):
                    valid_date_count += 1
            
            date_ratio = valid_date_count / len([v for v in sample_values if v is not None])
            
            is_date = date_ratio > 0.5
            
            if is_date:
                is_phi = True
                rule_detected = "DATE_OFFSET"
                self.logger.info(f"Date validated: {table_name}.{column_name}")
            else:
                PipelineRemarks += "+ COLUMN_NOT_PARSED_AS_DATE"
                self.logger.debug(f"Date validation failed: {table_name}.{column_name}")
            
            return is_phi, PipelineRemarks, rule_detected
            
        except Exception as e:
            self.logger.error(f"Error validating date {table_name}.{column_name}: {str(e)}")
            return is_phi, PipelineRemarks, rule_detected
    
    def _is_valid_date(self, value: Any) -> bool:
        """Check if a value represents a valid birth date"""
        try:
            # Try to parse as date
            if isinstance(value, datetime):
                date_obj = value
            else:
                # Try various date formats
                date_str = str(value).strip()
                date_formats = [
                    '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S',
                    '%d-%b-%Y', '%d-%B-%Y', '%d %b %Y', '%d %B %Y',
                    '%d-%b', '%d-%B', '%d %b', '%d %B',
                    '%b %d, %Y', '%B %d, %Y', '%b %d %Y', '%B %d %Y',
                    '%d %b, %Y', '%d %B, %Y',
                    '%Y/%m/%d', '%d.%m.%Y', '%d %m %Y',
                    '%Y.%m.%d', '%Y %b %d', '%Y %B %d',
                    '%b %d', '%B %d', '%d %b', '%d %B',
                    '%m-%d-%Y', '%m/%d/%y', '%d/%m/%y', '%d-%m-%Y', '%d-%m-%y',
                    '%Y/%b/%d', '%Y/%B/%d', '%d/%b/%Y', '%d/%B/%Y',
                    '%d %B', '%d %b', '%B %d', '%b %d',
                    '%Y', '%b %Y', '%B %Y',
                    '%d %B %Y', '%d %b %Y',  # e.g., 1 May 2025, 1 May 2024
                    '%d %B, %Y', '%d %b, %Y',  # e.g., 1 May, 2025
                    '%d %B', '%d %b',  # e.g., 1 May, 1 May
                    '%B %d, %Y', '%b %d, %Y',  # e.g., May 1, 2025
                    '%B %d %Y', '%b %d %Y',    # e.g., May 1 2025
                    '%B %d', '%b %d',          # e.g., May 1
                ]
                date_obj = None
                
                for fmt in date_formats:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if date_obj is None:
                    return False
            
            current_year = datetime.now().year
            return 1800 <= date_obj.year <= current_year
            
        except Exception:
            return False


class ZipcodeValidator(BasePHIValidator):
    """Validator for Zipcode columns"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_database_manager = mapping_database_manager
    
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        rule_detected = None
        PipelineRemarks = None
        is_phi = False
        try:
            self.logger.info(f"Validating ZipCode: {table_name}.{column_name}")
            
            if self.db_manager.check_column_type(table_name, column_name, 'numeric'):
                PipelineRemarks = "DATETYPE"  
            elif self.db_manager.check_column_type(table_name, column_name, 'string'):
                PipelineRemarks = "DATETYPE"
            else:
                PipelineRemarks = "NOT_NUMERIC + NOT_STRING"
            
            # Check column name patterns
            zipcode_patterns = [
                r'.*zip.*code.*', r'.*zip.*', r'.*postal.*code.*',
                r'.*post.*code.*'
            ]
            
            column_lower = column_name.lower()
            name_match = any(re.match(pattern, column_lower) for pattern in zipcode_patterns)
            
            # Get sample values to analyze
            sample_values = self.db_manager.get_sample_values(table_name, column_name, SAMPLE_SIZE)
            
            if not sample_values:
                self.logger.warning(f"No sample values found for {table_name}.{column_name}")
                PipelineRemarks += "+ ALL_VALUES_ARE_NONE"
                return is_phi, PipelineRemarks, rule_detected
            
            # Check if values match US zipcode patterns
            valid_zipcode_count = 0
            for value in sample_values:
                if value is not None and self._is_valid_zipcode(value):
                    valid_zipcode_count += 1
            
            zipcode_ratio = valid_zipcode_count / len([v for v in sample_values if v is not None])
            zipcode_ratio_pass = zipcode_ratio == 1
            
            # Consider it zipcode if name matches OR high percentage of valid zipcodes
            is_zipcode = name_match or zipcode_ratio_pass
            if zipcode_ratio_pass:
                PipelineRemarks += "+ VALUES_MATCH_FAILED"
            PipelineRemarks += "+ PATTERN_MATCHED" if name_match else "PATTERN_NOT_MATCHED"
            rule_detected = "ZIPCODE" if is_zipcode else None
            return is_phi, PipelineRemarks, rule_detected
            
        except Exception as e:
            self.logger.error(f"Error validating zipcode {table_name}.{column_name}: {str(e)}")
            return is_phi, PipelineRemarks, rule_detected
    
    def _is_valid_zipcode(self, value: Any) -> bool:
        """Check if a value represents a valid US zipcode"""
        try:
            zip_str = str(value).strip()
            # US zipcode patterns: 12345 or 12345-6789
            zipcode_pattern = r'^\d{5}(-\d{4})?$'
            return re.match(zipcode_pattern, zip_str) is not None
        except Exception:
            return False


class MaskValidator(BasePHIValidator):
    """Validator for columns that need masking (names, addresses, etc.)"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_database_manager = mapping_database_manager
        self.patient_table_config = config.get('patient_table', {})
    
    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        try:
            self.logger.info(f"Validating Mask requirement: {table_name}.{column_name}")
            return True, llm_result, "MASK"
        except Exception as e:
            self.logger.error(f"Error validating mask requirement {table_name}.{column_name}: {str(e)}")
            return False, "", None
    

class NotesValidator(BasePHIValidator):
    """Validator for clinical notes and unstructured text columns"""

    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        super().__init__(config)
        self.db_manager = database_manager
        self.mapping_database_manager = mapping_database_manager

    def validate(self, db_name: str, table_name: str, column_name: str, llm_result: LLMResult) -> tuple[bool, str, str]:
        return True, llm_result, "NOTES"



class PHIValidationToolsManager:
    """Manager class for all PHI validation tools"""
    
    def __init__(self, config: Dict[str, Any], database_manager, mapping_database_manager):
        """
        Initialize validation tools manager
        
        Args:
            config: Configuration dictionary
            database_manager: Database manager instance
        """
        self.config = config
        self.db_manager = database_manager
        self.mapping_database_manager = mapping_database_manager
        self.logger = logging.getLogger(__name__)
        
        # Load mapping cache only if not already loaded
        if not is_mapping_cache_loaded():
            self.logger.info("Mapping cache not loaded, initializing...")
            _load_mapping_cache(config, mapping_database_manager)
        else:
            self.logger.info("Mapping cache already loaded, skipping initialization")

        # Initialize all validators
        self.validators = {
            'patientid': PatientIDValidator(config, database_manager, mapping_database_manager),
            'encounterid': EncounterIDValidator(config, database_manager, mapping_database_manager),
            'dob': DOBValidator(config, database_manager, mapping_database_manager),
            'zipcode': ZipcodeValidator(config, database_manager, mapping_database_manager),
            'mask': MaskValidator(config, database_manager, mapping_database_manager),
            'notes': NotesValidator(config, database_manager, mapping_database_manager),
            'date': DateValidator(config, database_manager, mapping_database_manager)
        }
    
    def validate_phi_type(self, phi_type: str, db_name: str, table_name: str, column_name: str, llmremarks) -> bool:
        """
        Validate a specific PHI type for a column
        
        Args:
            phi_type: Type of PHI to validate
            db_name: Database name
            table_name: Table name
            column_name: Column name
            
        Returns:
            Boolean indicating if validation passed
        """
        try:
            if phi_type not in self.validators:
                self.logger.error(f"Unknown PHI type: {phi_type}")
                return False
            
            return self.validators[phi_type].validate(db_name, table_name, column_name, llmremarks)
            
        except Exception as e:
            self.logger.error(f"Error validating PHI type {phi_type} for {table_name}.{column_name}: {str(e)}")
            return False
