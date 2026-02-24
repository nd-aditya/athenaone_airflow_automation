import os
import django
import sys

# Set up Django environment
sys.path.append(
    "/Users/rohitchouhan/Documents/Code/backend/deidentification/deIdentification/"
)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "deIdentification.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

import pandas as pd
from core.process_df.unstruct.notes import NotesRule


# Dummy DB details object to simulate config
class DummyDBDetails:
    def get_pii_config(self):
        return {
            "config": {
                "mask": {
                    "PATIENT_FIRSTNAME": {
                        "masking_value": "((PATIENT_NAME_FM))",
                    },
                    "PATIENT_EMAIL": {
                        "masking_value": "((PATIENT_EMAIL))",
                    },
                    "PATIENT_LASTNAME": {
                        "masking_value": "((PATIENT_LASTNAME_FM))",
                    },
                },
                "dob": {
                    "PATIENT_DOB": {
                        "masking_value": "((DOB))",
                    },
                    "FATHER_DOB": {
                        "masking_value": "((FATHERSDOB))",
                    },
                    "MOTHER_DOB": {
                        "masking_value": "((MOTHERSDOB))",
                    },
                },
                "combine": {
                    "fullname": {
                        "combine": ["PATIENT_FIRSTNAME", "PATIENT_LASTNAME"],
                        "masking_value": "((PATIENT_NAME_FC))",
                    }
                },
                "replace_value": [
                    {"old_value": "ABC Hospital", "new_value": "((FACILITY_NAME))"},
                    {"old_value": "jaimatadi", "new_value": "((REPLACEMASK))"},
                ],
            }
        }

    def get_pii_db_config(self):
        # Not used in dummy setup
        return {}

    def get_secondary_pii_configs(self):
        return {}


# Create dummy main DataFrame with text containing PII
df_main = pd.DataFrame(
    {
        "patient_id": [100, 100, 100, 100],
        "_resolved_patient_id": [100, 100, 100, 100],
        "_resolved_nd_patient_id": [1000, 1000, 1000, 1000],
        "note_text": [
            "Mr. O'Brien, Mr. O'BRIEN, CHOUHAN, Rohitt, Rohith Chauhan, Chohan, chhouhan, chouhaan",
            "ROHIT, C-HOU-HAN,  Patient Jane's email is jane@example.com ",
            "C'HOUHAN , DOB is 1990-01-01 for Jane, Patient name: Jane",
            "R'OHIT , ABC Hospital is where John was born john@example.com, jaimatadi",
        ],
    }
)

# Create dummy pii_data_df that would normally come from database
dummy_pii_df = pd.DataFrame(
    {
        "nd_patient_id": [1000, 1000, 1000, 1000, 1000, 1000],
        "PATIENT_FIRSTNAME": ["ROHIT", "CHOUHAN", "OBRIEN", "AKRIVELLIS", "John", "Jane"],
        "PATIENT_LASTNAME": ["D", None, "Wick", "Watson", "Wayne", "Chilwal"],
        "PATIENT_EMAIL": [
            "john@example.com",
            "jane@example.com",
            "john2@example.com",
            " ",
            "john3@example.com",
            "karanchilwal@gmail.com",
        ],
        "PATIENT_DOB": [
            "1990-01-01",
            "1992-02-02",
            "1990-01-01",
            "1990-01-01",
            "1985-05-05",
            "1928-12-04",
        ],
        "FATHER_DOB": [
            "1967-01-01",
            "1992-02-02",
            "1990-01-01",
            "1990-01-01",
            "1985-05-05",
            "1928-12-04",
        ],
        "MOTHER_DOB": [
            "12-31-1976",
            "31-12-1977",
            "1990-01-01",
            "1990-01-01",
            "1985-05-05",
            "1928-12-04",
        ],
    }
)


# Patch NotesRule to inject dummy PII data
class TestNotesRule(NotesRule):

    def _get_pii_data_table(self, patient_ids: list):
        # Simulate loading filtered PII data for requested patients
        self.pii_data_df = dummy_pii_df[dummy_pii_df["nd_patient_id"].isin(patient_ids)]


# Instantiate and apply the rule
notes_rule = TestNotesRule(
    DummyDBDetails(),
    key_phi_columns=([], ["patient_id"], [], []),
    possible_patient_identifier_columns=["patient_id"],
)

# Apply the rule
result_df = notes_rule.apply(
    df_main.copy(),
    {
        "column_name": "note_text",
        "is_phi": True,
        "de_identification_rule": "NOTES",
        "add_to_phi_table": False,
        "column_name_for_phi_table": None,
        "ignore_rows": {},
        "reference_mapping": {},
    },
)

# Show result
# print(df_main)
# print(dummy_pii_df)
pd.set_option("display.max_colwidth", None)
print(result_df[["note_text"]])

# print(result_df)
