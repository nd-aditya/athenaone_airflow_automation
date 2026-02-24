from .base import CorrectnessValidator
import re
from datetime import datetime
import pandas as pd


class AgeCorrectness(CorrectnessValidator):
    def validate(self):
        today = datetime.today()

        def compute_age(dob):
            if pd.isnull(dob):
                return None
            try:
                return (today - pd.to_datetime(dob)).days // 365
            except:
                return None

        self.df["age"] = self.df["dob"].apply(compute_age)
        self.df["invalid_age"] = self.df["age"].apply(
            lambda x: x is None or x < 0 or x > 120
        )
        return self.df[self.df["invalid_age"]]


class ZipCodeCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[~self.df["zip_code"].astype(str).str.match(r"^\d{5}$")]


class DobCorrectness(CorrectnessValidator):
    def validate(self):
        def is_valid_dob(dob):
            try:
                d = pd.to_datetime(dob)
                return d < datetime.today()
            except:
                return False

        return self.df[~self.df["dob"].apply(is_valid_dob)]


class PatientIdCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[self.df["patient_id"].isnull() | (self.df["patient_id"] == "")]


class EncounterIdCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[
            self.df["encounter_id"].isnull() | (self.df["encounter_id"] == "")
        ]


class IcdCodesCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[
            ~self.df["icd_code"].astype(str).str.match(r"^[A-Z]\d{2}(\.\d{1,4})?$")
        ]


class PhoneNumberCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[
            ~self.df["phone"]
            .astype(str)
            .str.match(r"^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}(\s*x\d+)?$")
        ]


class EmailCorrectness(CorrectnessValidator):
    def validate(self):
        pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
        return self.df[~self.df["email"].astype(str).str.match(pattern)]


class SexValueCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[
            ~self.df["sex"].str.lower().isin(["male", "female", "other", "unknown"])
        ]


class SSNCorrectness(CorrectnessValidator):
    def validate(self):
        return self.df[~self.df["ssn"].astype(str).str.match(r"^\d{3}-\d{2}-\d{4}$")]


class StateAbbreviationCorrectness(CorrectnessValidator):
    def validate(self):
        us_states = set(
            [
                "AL",
                "AK",
                "AZ",
                "AR",
                "CA",
                "CO",
                "CT",
                "DE",
                "FL",
                "GA",
                "HI",
                "ID",
                "IL",
                "IN",
                "IA",
                "KS",
                "KY",
                "LA",
                "ME",
                "MD",
                "MA",
                "MI",
                "MN",
                "MS",
                "MO",
                "MT",
                "NE",
                "NV",
                "NH",
                "NJ",
                "NM",
                "NY",
                "NC",
                "ND",
                "OH",
                "OK",
                "OR",
                "PA",
                "RI",
                "SC",
                "SD",
                "TN",
                "TX",
                "UT",
                "VT",
                "VA",
                "WA",
                "WV",
                "WI",
                "WY",
            ]
        )
        return self.df[~self.df["state"].str.upper().isin(us_states)]
