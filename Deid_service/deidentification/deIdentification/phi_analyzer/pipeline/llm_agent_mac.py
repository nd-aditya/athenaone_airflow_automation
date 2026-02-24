"""
LLM Agent for PHI Classification
Handles LLM-based classification of columns for PHI detection
"""

import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from phi_analyzer.pipeline.llm_models.models import LLMManager

@dataclass
class PHIClassificationResult:
    """Data class for PHI classification results"""
    is_phi: str  # 'yes' or 'no'
    phi_type: str  # Type of PHI if detected
    remarks: str
    confidence: Optional[float] = None
    reasoning: Optional[str] = None


class LLMAgent:
    """LLM Agent for PHI classification using OpenAI API"""
    
    def __init__(self, llm_config: Dict[str, Any]):
        """
        Initialize LLM Agent with configuration
        
        Args:
            config: LLM configuration dictionary
        """
        self.llm_config = llm_config
        self.logger = logging.getLogger(__name__)
        
        self.llm_client_manager = LLMManager()
        self._setup_client()
    
    def _setup_client(self) -> None:
        """Setup LLM client"""
        try:
            
            self.model_name = self.llm_config.get('model_name', 'gpt-4')
            self.logger.info(f"Looking for model: {self.model_name}")
            
            # Debug: print available models
            available_models = list(self.llm_client_manager.models.keys())
            self.logger.info(f"Available models: {available_models}")
            
            self.llm_client = self.llm_client_manager.get_tool(self.model_name)
            
            if self.llm_client is None:
                raise ValueError(f"Failed to get LLM tool for model: {self.model_name}. Available models: {available_models}")
            
            self.logger.info(f"LLM Agent initialized with model: {self.model_name}")
        except Exception as e:
            self.logger.error(f"Failed to setup LLM client: {str(e)}")
            self.llm_client = None  # Ensure it's set to None on failure
            raise
    
    def _create_phi_classification_prompt(self, column_name: str, sample_values: List[Any], table_name: str) -> str:
        """
        Create detailed prompt for PHI classification
        
        Args:
            column_name: Name of the column to analyze
            sample_values: Sample values from the column
            table_name: Name of the table containing the column
            
        Returns:
            Formatted prompt string
        """
        
        # Convert sample values to string representation
        sample_str = []
        for i, value in enumerate(sample_values[:10]):  # Limit to 10 samples for prompt
            if value is not None:
                sample_str.append(f"  {i+1}. {repr(value)}")
        
        samples_text = "\n".join(sample_str) if sample_str else "  No non-null values found"
        
        prompt = f"""
You are a PHI (Protected Health Information) classification expert for Electronic Health Records (EHR) de-identification.

Your task is to analyze a database column and determine:
1. Whether it contains PHI information (yes/no)
2. If yes, classify the specific type of PHI

**CRITICAL INSTRUCTIONS:**
- Return ONLY a JSON response with no additional text or commentary
- Be precise and consistent with the classification types
- Consider both column name patterns and sample values
- The sample values are stripped to restrict the prompt length, so keep that in mind when classifying.
- Remember that 'State' is NOT considered PHI

**COLUMN INFORMATION:**
Table Name: {table_name}
Column Name: {column_name}
Sample Values:
{samples_text}

**PHI CLASSIFICATION TYPES:**

1. **patientid**: Unique patient identifiers
   - Column names typically: patientid, pid, chartid, userid, patient_id
   - Values: Usually numeric or alphanumeric unique identifiers
   - JSON Response: {{"is_phi": "yes", "phi_type": "patientid", "remarks": ""}}

2. **encounterid**: Unique encounter identifiers  
   - Column names typically: encounterid, eid, encid, clinicalencounterid, encounter_id,visitid
   - Values: Usually numeric or alphanumeric unique identifiers
   - JSON Response: {{"is_phi": "yes", "phi_type": "encounterid","remarks": ""}}

3. **dob**: Date of birth information
   - Column names typically: dob, patientdob, date_of_birth, birth_date
   - Values: Date values representing birth dates
   - JSON Response: {{"is_phi": "yes", "phi_type": "dob","remarks": ""}}

4. **zipcode**: ZIP/Postal codes
   - Column names typically: zipcode, zip, postal_code, zip_code
   - Values: US ZIP codes (5 digits, or 5+4 format)
   - JSON Response: {{"is_phi": "yes", "phi_type": "zipcode","remarks": ""}}

5. **mask**: Personal identifiable information requiring masking
   - Column names typically: first_name, last_name, middle_name, name, address, city, phone, email, ssn, blockid, pubtime, urls, ipaddress, password
   - Values: Names, addresses, phone numbers, emails, or other personal info, institution name, urls, ipaddress, amounts, filepaths, password
   - Doctor and Provider information should not be classified as mask.
   - All amount/billing related info like amount, charge, billed_amount, claim_amount, payment_amount, total_amount, balance, fee, invoice_amount, paid_amount, cost, allowed_amount, adjudicated_amount, patient due amount, patient balance, insurance amount due etc. should be masked.
   - Insurance related fields like insid, insurance_id, insurer_id, insurer_name, policy_number, policy_no, member_id, subscriber_id, subscriber_no, plan_id, ins_policy_number, InsId, PreCertificationNum should be masked.
   - Facility names, facility addresses, facility phone numbers, facility emails, facility ssn, facility blockid, facility pubtime, facility urls, facility ipaddress, facility amounts, facility filepaths, facility password should be classified as mask.
   - So all values that contain personal identifiable information and that falls in this should be classified as mask.
   - Note: 'State' is NOT PHI and should not be classified as mask
   - JSON Response: {{"is_phi": "yes", "phi_type": "mask","remarks": ""}}

6. **notes**: Unstructured text/clinical notes
   - Column names: Various (notes, comments, clinical_notes, narrative, etc.)
   - Values: Long unstructured text, clinical narratives, free-text entries
   - Characteristics: Typically longer text entries with medical terminology
   - In notes, give remarks also, like which type of notes it is, like 'FREE_TEXT','BLOB','BINARY','ENCRYPTED','PDF','XML','XML ENCRYPTED','IMAGE'.
   - JSON Response: {{"is_phi": "yes", "phi_type": "notes","remarks": "FREE_TEXT or BLOB or BINARY or ENCRYPTED or PDF or XML or XML ENCRYPTED or IMAGE"}}

7. **date**: Date and timestamp fields (excluding DOB)
   - Column names typically: date, time, timestamp, created_date, updated_date, encounter_date, admission_date, etc.
   - Values: Date/time values, timestamps
   - JSON Response: {{"is_phi": "yes", "phi_type": "date","remarks": ""}}

**NON-PHI CLASSIFICATION:**
- If the column does not contain PHI information
- JSON Response: {{"is_phi": "no", "phi_type": "","remarks": ""}}

**EXAMPLES:**

Column: patient_id, Samples: [12345, 67890, 11111]
Response: {{"is_phi": "yes", "phi_type": "patientid","remarks": ""}}

Column: state, Samples: ["CA", "NY", "TX"]  
Response: {{"is_phi": "no", "phi_type": "","remarks": ""}}

Column: first_name, Samples: ["John", "Jane", "Michael"]
Response: {{"is_phi": "yes", "phi_type": "mask","remarks": ""}}

Column: clinical_notes, Samples: ["Patient presents with chest pain...", "History of diabetes..."]
Response: {{"is_phi": "yes", "phi_type": "notes","remarks": "FREE_TEXT"}}

Column: encounter_date, Samples: ["2023-01-15", "2023-02-20"]
Response: {{"is_phi": "yes", "phi_type": "date","remarks": ""}}

**ANALYZE THE PROVIDED COLUMN AND RETURN ONLY THE JSON RESPONSE:** <no think>
"""
#         prompt = f"""
# You are a PHI (Protected Health Information) classification expert for Electronic Health Records (EHR) de-identification.

# Your task is to analyze a database column and determine:

# 1. Whether it contains PHI information (yes/no)
# 2. If yes, classify the specific type of PHI using **only** the allowed phi_type values: patientid, encounterid, dob, zipcode, mask, notes, date

# **CRITICAL INSTRUCTIONS (must be followed exactly):**

# * **Return ONLY a single JSON object** and nothing else (no explanation, no extra text, no code fences). The output must exactly match one of these forms:

#   * {{ "is_phi": "yes", "phi_type": "patientid", "remarks": "" }}
#   * {{ "is_phi": "yes", "phi_type": "encounterid", "remarks": "" }}
#   * {{ "is_phi": "yes", "phi_type": "dob", "remarks": "" }}
#   * {{ "is_phi": "yes", "phi_type": "zipcode", "remarks": "" }}
#   * {{ "is_phi": "yes", "phi_type": "mask", "remarks": "" }}
#   * {{ "is_phi": "yes", "phi_type": "notes", "remarks": "\<FREE_TEXT|BLOB|BINARY|ENCRYPTED|PDF|XML|XML ENCRYPTED|IMAGE>" }}
#   * {{ "is_phi": "yes", "phi_type": "date", "remarks": "" }}
#   * {{ "is_phi": "no",  "phi_type": "","remarks": "" }}
# * Use **exact** string values for `is_phi` ("yes" or "no"), `phi_type` (one of the allowed values or empty), and a short string for `remarks` (or empty).
# * **Do not** add other keys or change key order/format.
# * Matching must be **case-insensitive**. Normalize column and table names by lowercasing and removing spaces, dashes, and punctuation before matching (e.g., "Patient_ID", "patientId", "patient-id" -> "patientid").

# **DECISION PRIORITY (apply in this order):**

# 1. **Exact normalized column-name match** against known PHI patterns (lists below) → choose that phi_type.
# 2. **Prefix/suffix matches** (e.g., endswith "_id", startswith "date", contains "dob", contains "zip") if no exact match.
# 3. **Table-name context** (if table name contains tokens like "patient", "encounter", "claim", "visit", "billing", use that to disambiguate generic names like "id" or "code").
# 4. **Sample values**: use samples to confirm or override name-based inference **only** when samples provide clear, strong evidence (well-formed ISO dates, full personal names, emails, phone patterns, "%PDF-", long readable clinical text, base64/image headers, etc.).

#    * If samples are short/truncated/stripped, **prefer column-name / table-name** decisions.
# 5. If column-name and samples disagree and both are strong, choose the source with clearer evidence (long readable samples trump a generic name; a precise column name with strong semantic match trumps ambiguous samples).

# **IMPORTANT EDGE RULES:**

# * **State is NOT PHI.** Column names like `state`, `us_state`, `state_code` MUST be classified as non-PHI: {{ "is_phi":"no","phi_type":"","remarks":"" }}.
# * **Doctor/provider names and provider IDs**: do **not** classify as patient `mask` for PHI. Provider/doctor identifiers are treated as non-PHI in this schema unless the sample values clearly indicate patient-identifying content (rare).
# * **Facility information** (facility name, facility address, facility phone, facility email, facility identifiers) **is** `mask`.
# * **Samples may be truncated**; if samples look incomplete or are short, favor column/table name rules.

# **PHI PATTERN LISTS & DETECTION HEURISTICS (use these exact mappings):**

# 1. **patientid**

#    * Column-name tokens (normalized): patientid, patient_id, pid, patid, patientno, chartid, mrn, medical_record_number, recordid, record_id, userid, user_id
#    * Typical values: short numeric or alphanumeric unique IDs (e.g., 12345, A00123).
#    * Return: {{ "is_phi": "yes", "phi_type": "patientid", "remarks": "" }}

# 2. **encounterid**

#    * Column-name tokens: encounterid, encounter_id, encid, eid, visitid, visit_id, clinicalencounterid, encounternumber
#    * Typical values: alphanumeric visit/encounter ids.
#    * Return: {{ "is_phi": "yes", "phi_type": "encounterid", "remarks": "" }}

# 3. **dob**

#    * Column-name tokens: dob, date_of_birth, birth_date, patientdob, birthdate
#    * Value patterns: full birthdates (YYYY-MM-DD, MM/DD/YYYY, etc.) or explicit birth-year values.
#    * Return: {{ "is_phi": "yes", "phi_type": "dob", "remarks": "" }}

# 4. **zipcode**

#    * Column-name tokens: zip, zipcode, postal_code, zip_code, postalcode
#    * Value patterns: 5-digit or 5+4 US ZIP formats.
#    * Return: {{ "is_phi": "yes", "phi_type": "zipcode", "remarks": "" }}

# 5. **mask**  — broad class for direct identifiers, contact info, amounts, insurance, facility info

#    * **Personal identifiers / contacts / direct PII**:

#      * first_name, last_name, middle_name, name, patient_name, address, street, city (when part of address), phone, phone_number, telephone, email, ssn, social_security_number, ipaddress, ip_address, url, urls, filepath, password
#    * **Facility & institution**:

#      * facility_name, facility_address, facility_phone, facility_email, facility_ssn, facility_blockid, facility_urls, facility_ipaddress, facility_amounts, facility_filepaths
#    * **Financial / Amount fields (NEW & STRICT)** — these **must** be masked:

#      * amount, charge, billed_amount, claim_amount, payment_amount, total_amount, balance, fee, invoice_amount, paid_amount, cost, allowed_amount, adjudicated_amount
#    * **Insurance-related fields (NEW & STRICT)** — these **must** be masked:

#      * insid, insurance_id, insurer_id, insurer_name, policy_number, policy_no, member_id, subscriber_id, subscriber_no, plan_id, ins_policy_number, InsId
#      * In claims/billing tables (table name contains 'claim', 'claims', 'billing', 'invoice'), any insurer/insurance-related id or number should be classified as `mask`.
#    * **SSN / national IDs**: ssn, national_id, nin -> mask
#    * **If column is clearly facility contact/info => mask.**
#    * **NOTE:** Doctor/provider names and provider IDs are **not** patient `mask` (treat as non-PHI for patient masking).
#    * Return: {{ "is_phi": "yes", "phi_type": "mask", "remarks": "" }}

# 6. **notes** — unstructured clinical text, documents, attachments

#    * Column-name tokens: notes, comment, comments, clinical_notes, narrative, note_text, report_text, document_text, free_text, observation_text
#    * Decide subtype and set `remarks` to exactly one of:

#      * FREE_TEXT: human-readable clinical sentences, long narratives.
#      * PDF: sample begins with "%PDF-" or contains PDF header/text.
#      * XML: sample begins with "\<?xml" or obvious XML tags.
#      * XML ENCRYPTED: XML-looking content that is encrypted or wrapped in encrypted blocks.
#      * ENCRYPTED: contains "ENCRYPTED", PGP headers, or clear encryption markers.
#      * IMAGE: base64 starting with "data\:image/" or image file headers.
#      * BLOB/BINARY: long base64 strings or binary dumps without readable text.
#    * Choose the subtype that best matches sample evidence. If samples are clearly long clinical sentences -> FREE_TEXT.
#    * Return example: {{ "is_phi": "yes", "phi_type": "notes", "remarks": "FREE_TEXT" }}

# 7. **date** — general date/time fields (exclude DOB)

#    * Column-name tokens: date, time, timestamp, created_date, updated_date, encounter_date, admission_date, discharge_date, appointment_time, visit_date, modified_at
#    * Value patterns: ISO timestamps, "YYYY-MM-DD", "YYYY-MM-DD HH\:MM\:SS", epoch numbers.
#    * Return: {{ "is_phi": "yes", "phi_type": "date", "remarks": "" }}

# **NON-PHI:**

# * If none of the patterns above match and the column values appear to be codes, categories, numeric measurements (height/weight without identifiers), or `state` fields, return:

#   * {{ "is_phi": "no", "phi_type": "", "remarks": "" }}

# **TIE-BREAKER RULES (recap):**

# * If column is `"id"` or `"code"`: use table_name token to decide (patient -> patientid, encounter/visit -> encounterid, claim/billing -> mask for claim/insurer ids).
# * If column-name strongly matches a PHI pattern but samples seem short/confusing, **prefer column-name** (samples may be truncated).
# * If samples clearly show a different strong pattern (well-formed date strings, "%PDF-", long readable clinical text, email pattern, phone pattern), let samples override.

# ---

# ## INPUT–OUTPUT EXAMPLES:

# ### Example 1

# **Column:** patient_id
# **Samples:** [12345, 67890, 11111]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "patientid", "remarks": "" }}

# ---

# ### Example 2

# **Column:** state
# **Samples:** ["CA", "NY", "TX"]
# **Output:**
# {{ "is_phi": "no", "phi_type": "", "remarks": "" }}

# ---

# ### Example 3

# **Column:** first_name
# **Samples:** ["John", "Jane", "Michael"]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "mask", "remarks": "" }}

# ---

# ### Example 4

# **Column:** clinical_notes
# **Samples:** ["Patient presents with chest pain...", "History of diabetes..."]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "notes", "remarks": "FREE_TEXT" }}

# ---

# ### Example 5

# **Column:** encounter_date
# **Samples:** ["2023-01-15", "2023-02-20"]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "date", "remarks": "" }}

# ---

# ### Example 6 (Insurance masking)

# **Table:** claims
# **Column:** insid
# **Samples:** ["INS-12345", "INS-67890"]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "mask", "remarks": "" }}

# ---

# ### Example 7 (Financial masking)

# **Table:** claims
# **Column:** billed_amount
# **Samples:** ["500.00", "1200.50"]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "mask", "remarks": "" }}

# ---

# ### Example 8 (PDF notes)

# **Column:** report_file
# **Samples:** ["%PDF-1.7 ..."]
# **Output:**
# {{ "is_phi": "yes", "phi_type": "notes", "remarks": "PDF" }}

# ---

# **OUTPUT REQUIREMENT (final):**

# * Analyze the inputs below and **output ONLY the single JSON object** matching the allowed formats above.

# COLUMN INFORMATION (use these fields to make the decision):
# Table Name: {table_name}
# Column Name: {column_name}
# Sample Values:
# {samples_text}
# """
        return prompt
    
    def classify_columns(self, column_data: List[Dict[str, Any]]) -> List[PHIClassificationResult]:
        """
        Batch classify multiple columns for PHI content using LLM
        
        Args:
            column_data: List of dicts containing table_name, column_name, sample_values
        
        Returns:
            List of PHIClassificationResult objects
        """
        try:
            if self.llm_client is None:
                raise ValueError("LLM client not initialized. Check your configuration and model setup.")
            
            system_prompt = "You are a PHI classification expert. Return only JSON responses with no additional text."

            # Pre-check: if all samples for a column are None/empty, short-circuit as no PHI
            results: List[PHIClassificationResult] = []
            prompts = []
            prompt_indices = []  # map from prompts index back to column_data index
            for idx, data in enumerate(column_data):
                samples = data.get('sample_values') or []
                non_null_samples = [v for v in samples if v is not None and str(v).strip() != '']
                if len(samples) > 0 and len(non_null_samples) == 0:
                    # All provided samples are None/empty -> mark as non-PHI without calling LLM
                    results.append(PHIClassificationResult(is_phi='no', phi_type='', remarks='ALL_VALUES_ARE_NONE'))
                    continue
                prompt = self._create_phi_classification_prompt(
                    data['column_name'],
                    data['sample_values'],
                    data['table_name']
                )
                full_prompt = system_prompt + '\n' + prompt
                prompts.append(full_prompt)
                prompt_indices.append(idx)
            
            # Send batch request
            response = []
            if prompts:
                response = self.llm_client.invoke({
                    "input_text": prompts,
                    "model_param": None,
                    "num_workers": 2,
                    "stream": False
                })

            for r_idx, resp_text in enumerate(response):
                idx = prompt_indices[r_idx]
                table_name = column_data[idx]['table_name']
                column_name = column_data[idx]['column_name']
                sample_values = column_data[idx]['sample_values']

                resp_text = resp_text.strip()
                self.logger.debug(f"LLM response for {table_name}.{column_name}: {resp_text}")
                
                try:
                    # Clean response
                    cleaned = resp_text.strip()
                    if cleaned.startswith("```"):
                        cleaned = cleaned.lstrip("`")
                        if cleaned.lower().startswith("json"):
                            cleaned = cleaned[4:]
                        if cleaned.endswith("```"):
                            cleaned = cleaned[:-3]
                    cleaned = cleaned.strip()

                    result_json = json.loads(cleaned)
                    
                    # Validate fields
                    if 'is_phi' not in result_json or 'phi_type' not in result_json:
                        raise ValueError("Missing required fields in LLM response")
                    
                    is_phi = result_json['is_phi'].lower()
                    phi_type = result_json['phi_type'].lower() if result_json['phi_type'] else ''
                    remarks = result_json['remarks'].lower() if result_json['remarks'] else ''
                    
                    if is_phi not in ['yes', 'no']:
                        self.logger.warning(f"Invalid is_phi value: {is_phi}, defaulting to 'no'")
                        is_phi, phi_type, remarks = 'no', '', ''

                    if (sample_values is None or len(sample_values) == 0) and 'ALL_VALUES_ARE_NONE' not in remarks:
                        remarks += '+ALL_VALUES_ARE_NONE'

                    result_obj = PHIClassificationResult(
                        is_phi=is_phi,
                        phi_type=phi_type,
                        remarks=remarks,
                        confidence=result_json.get('confidence'),
                        reasoning=result_json.get('reasoning')
                    )

                    # Post-check: if LLM says 'no' and remarks do NOT contain 'ALL_VALUES_ARE_NONE',
                    # run DateValidator to see if it's actually a date
                    if result_obj.is_phi == 'no' and ('all_values_are_none' not in (result_obj.remarks or '').lower()):
                        pipe_remark = remarks + " + LLM_PHI_NO_CHECKING_FOR_DATE"
                        result_obj = PHIClassificationResult(is_phi='yes', phi_type='date', remarks=(pipe_remark or '').lower())
                    results.append(result_obj)

                except Exception as e:
                    self.logger.error(f"Failed to parse LLM JSON response for {table_name}.{column_name}: {str(e)}")
                    self.logger.error(f"Raw response: {resp_text}")
                    results.append(PHIClassificationResult(is_phi='no', phi_type='', remarks=""))

            return results

        except Exception as e:
            self.logger.error(f"{traceback.format_exc()}")
            self.logger.error(f"Failed to batch classify columns: {str(e)}")
            return [PHIClassificationResult(is_phi='no', phi_type='', remarks="") for _ in column_data]

    
    def batch_classify_columns(self, column_data: List[Dict[str, Any]]) -> List[PHIClassificationResult]:
        """
        Classify multiple columns in batch using the underlying LLM tool's batch API.
        Falls back to sequential calls if batch is not supported.
        """
        if not column_data:
            return []

        try:
            if self.llm_client is None:
                raise ValueError("LLM client not initialized. Check your configuration and model setup.")
                
            prompts: List[str] = []
            for data in column_data:
                prompt = self._create_phi_classification_prompt(
                    data['column_name'], data.get('sample_values', []), data['table_name']
                )
                system_prompt = "You are a PHI classification expert. Return only JSON responses with no additional text."
                full_prompt = system_prompt + '\n' + prompt
                prompts.append(full_prompt)

            # Prefer vectorized batch invoke from LLMManager tool (LM Studio supports list inputs)
            responses = self.llm_client.invoke({
                "input_text": prompts,
                "model_param": None,
                "num_workers": min(len(prompts), 8),
                "stream": False
            })

            results: List[PHIClassificationResult] = []
            for resp_text in responses:
                text = (resp_text or "").strip()
                try:
                    cleaned = text.strip()
                    if cleaned.startswith("```"):
                        cleaned = cleaned.lstrip("`")
                        if cleaned.lower().startswith("json"):
                            cleaned = cleaned[4:]
                        if cleaned.endswith("```"):
                            cleaned = cleaned[:-3]
                    cleaned = cleaned.strip()
                    obj = json.loads(cleaned)
                    is_phi = str(obj.get('is_phi', 'no')).lower()
                    phi_type = str(obj.get('phi_type', '') or '').lower()
                    remarks = str(obj.get('remarks', '') or '').lower()
                    if is_phi not in ['yes', 'no']:
                        is_phi, phi_type, remarks = 'no', '', ''
                    results.append(PHIClassificationResult(
                        is_phi=is_phi,
                        phi_type=phi_type,
                        remarks=remarks,
                        confidence=obj.get('confidence'),
                        reasoning=obj.get('reasoning')
                    ))
                except Exception as e:
                    self.logger.error(f"Failed to parse batch LLM JSON response: {e} | Raw: {text}")
                    results.append(PHIClassificationResult(is_phi='no', phi_type='', remarks=""))

            return results

        except Exception as e:
            self.logger.warning(f"Batch invoke not available or failed, falling back to sequential: {e}")
            seq_results: List[PHIClassificationResult] = []
            for data in column_data:
                try:
                    seq_results.append(self.classify_column(
                        column_name=data['column_name'],
                        sample_values=data.get('sample_values', []),
                        table_name=data['table_name']
                    ))
                except Exception:
                    seq_results.append(PHIClassificationResult(is_phi='no', phi_type='', remarks=""))
            return seq_results
