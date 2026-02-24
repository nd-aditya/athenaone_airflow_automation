import ahocorasick
import threading


# class UniversalPIIDeIdentifier:

#     def __init__(self, text: str, universal_pii_data: dict, date_parse_cache: dict):
#         self.text = text
#         self.universal_pii_data = universal_pii_data
#         self.date_parse_cache = date_parse_cache

#     def deidentify(self):
#         nd_logger.info("Inside: UniversalPIIDeIdentifier")
#         for table_name, pii_data in tqdm(self.universal_pii_data.items(), desc="Processing Universal PII Data"):
#             metadata = pii_data.get("metadata", {})
#             mask_map = {value: metadata.get(col, f"(({col}))") for row in pii_data['rows'] for col, value in row.items()}
        
#             for old_value, new_value in tqdm(mask_map.items(), desc=f"De-identifying {table_name}"):
#                 processed_value = str(old_value).strip()
#                 if processed_value in [None, "", "None", "none", "null", " "] or len(str(processed_value))<1:
#                     continue
#                 self.text = self.text.replace(f" {str(old_value)} " , f" {str(new_value)} ")
#             nd_logger.info(f"UniversalPIIDeIdentifier completed for {table_name}")
    
#         return self.text


import re
from tqdm import tqdm
from deIdentification.nd_logger import nd_logger

import re
from tqdm import tqdm
from deIdentification.nd_logger import nd_logger
import threading

class UniversalPIIDeIdentifier:
    replacement_map = {}  # Global mapping of PII values -> masked values
    compiled_pattern = None  # Global compiled regex pattern
    build_lock = threading.Lock()  # Ensure thread safety during build

    def __init__(self, text: str, universal_pii_data: dict):
        self.text = text
        self.universal_pii_data = universal_pii_data

    @classmethod
    def build_pattern(cls, universal_pii_data):
        """Builds a global regex pattern once for efficient PII replacement."""
        if cls.compiled_pattern:  
            return  # Already built, no need to rebuild

        with cls.build_lock:  # Ensure only one thread builds it
            if cls.compiled_pattern:  
                return  # Another thread might have built it

            replacement_map = {}
            pii_values = []

            for table_name, pii_data in universal_pii_data.items():
                metadata = pii_data.get("metadata", {})
                for row in pii_data.get("rows", []):
                    for col, value in row.items():
                        processed_value = str(value).strip()
                        if processed_value and processed_value.lower() not in {"none", "null"}:
                            mask_value = metadata.get(col, f"(({col}))")
                            processed_value = f" {processed_value} "
                            replacement_map[processed_value] = f" {mask_value} "
                            pii_values.append(re.escape(processed_value))  # Escape for regex

            # Store the built structures globally
            cls.replacement_map = replacement_map
            cls.compiled_pattern = re.compile(r"\b(" + "|".join(pii_values) + r")\b", re.IGNORECASE)

    def deidentify(self):
        """Uses regex-based substitution for fast PII masking"""
        if not self.compiled_pattern:
            self.build_pattern(self.universal_pii_data)  # Ensure pattern is built

        nd_logger.info("UniversalPIIDeIdentifier - Starting De-identification")

        def mask_match(match):
            original = match.group(0)
            return self.replacement_map.get(original, original)

        self.text = self.compiled_pattern.sub(mask_match, self.text)

        nd_logger.info("UniversalPIIDeIdentifier - Completed")
        return self.text

class UniversalPIIDeIdentifier1:
    automaton = ahocorasick.Automaton()
    automaton_lock = threading.Lock()
    automaton_ready = threading.Event()  # Used to signal completion

    @classmethod
    def build_automaton(cls, universal_pii_data):
        """Thread-safe method to build the Aho-Corasick Trie."""
        if cls.automaton_ready.is_set():
            return  # Trie is already built, no need to rebuild

        with cls.automaton_lock:  # Prevent multiple threads from building at once
            if cls.automaton_ready.is_set():
                return  # Another thread finished building
            
            num_entries = 0  # Track added words
            for table_name, pii_data in universal_pii_data.items():
                metadata = pii_data.get("metadata", {})
                for row in pii_data.get("rows", []):
                    for col, value in row.items():
                        processed_value = str(value).strip()
                        if processed_value and processed_value.lower() not in {"none", "null"}:
                            mask_value = metadata.get(col, f"(({col}))")
                            cls.automaton.add_word(f" {processed_value} ", f" {mask_value} ")
                            num_entries += 1

            cls.automaton.make_automaton()  # Finalize the Trie
            cls.automaton_ready.set()  # Signal completion
            print(f"Aho-Corasick Trie built successfully with {num_entries} entries.")

    @classmethod
    def deidentify(cls, text: str):
        """Replaces PII terms in text using the globally built Aho-Corasick Trie."""
        cls.automaton_ready.wait()  # Wait for Trie to be built

        output = []
        last_pos = 0
        for end_index, mask_value in cls.automaton.iter(text):
            start_index = end_index - len(mask_value) + 1
            output.append(text[last_pos:start_index])  # Add text before match
            output.append(mask_value)  # Add masked value
            last_pos = end_index + 1

        output.append(text[last_pos:])  # Append remaining text
        return "".join(output)



# class UniversalPIIDeIdentifier:
#     automaton = ahocorasick.Automaton()

#     @classmethod
#     def build_automaton(cls, universal_pii_data):
#         """Builds a global Aho-Corasick Trie with PII terms for fast search."""
#         if len(cls.automaton) > 0:
#             return
#         for table_name, pii_data in universal_pii_data.items():
#             metadata = pii_data.get("metadata", {})
#             for row in pii_data.get("rows", []):
#                 for col, value in row.items():
#                     processed_value = str(value).strip()
#                     if processed_value and processed_value.lower() not in {"none", "null"}:
#                         mask_value = metadata.get(col, f"(({col}))")
#                         cls.automaton.add_word(f" {processed_value} ", f" {mask_value} ")

#         cls.automaton.make_automaton()  # Finalize the Trie
#         nd_logger.info("Aho-Corasick Trie built successfully.")
    

#     @classmethod
#     def deidentify(cls, text: str):
#         """Replaces PII terms in text using the globally built Aho-Corasick Trie."""
#         output = []
#         last_pos = 0

#         for end_index, mask_value in UniversalPIIDeIdentifier.automaton.iter(text):
#             start_index = end_index - len(mask_value) + 1
#             output.append(text[last_pos:start_index])  # Add text before match
#             output.append(mask_value)  # Add masked value
#             last_pos = end_index + 1

#         output.append(text[last_pos:])  # Append remaining text
#         return "".join(output)

