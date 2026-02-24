from sqlalchemy import create_engine, text
import pandas as pd
from faker import Faker
import random
from datetime import datetime
import time

# ================================
# CONFIG
# ================================
SERVER = "localhost:1433"
DATABASE = "texas_20251210"
USERNAME = "sa"
PASSWORD = "ndAdmin2025"
NEW_ROWS = 5  # change this as needed

fake = Faker()

connection_str = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
engine = create_engine(connection_str)

print("\n===== Running Incremental Insert =====")

# ================================
# GET CURRENT MAX IDs
# ================================
def get_max_id(table, id_col):
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT ISNULL(MAX({id_col}),0) FROM {table}")).scalar()
        return result

max_patient = get_max_id("Patient", "PatientID")
max_facility = get_max_id("Facility", "FacilityID")
max_encounter = get_max_id("Encounter", "EncounterID")
max_vital = get_max_id("Vitals", "VitalID")
max_note = get_max_id("ProgressNotes", "NoteID")

print(f"Current Max IDs -> Patient:{max_patient}, Facility:{max_facility}, Encounter:{max_encounter}")

# ================================
# GENERATE INCREMENTAL DATA
# ================================
df_patient = pd.DataFrame([
    {
        "PatientID": max_patient + i,
        "Name": fake.name(),
        "Gender": random.choice(["Male", "Female"]),
        "DOB": fake.date_of_birth(),
        "CreatedAt": datetime.now(),
    }
    for i in range(1, NEW_ROWS + 1)
])

df_facility = pd.DataFrame([
    {
        "FacilityID": max_facility + i,
        "Name": fake.company(),
        "City": fake.city(),
        "State": fake.state(),
        "CreatedAt": datetime.now(),
    }
    for i in range(1, NEW_ROWS + 1)
])

df_encounter = pd.DataFrame([
    {
        "EncounterID": max_encounter + i,
        "PatientID": random.randint(1, max_patient + NEW_ROWS),
        "FacilityID": random.randint(1, max_facility + NEW_ROWS),
        "EncounterDate": datetime.now(),
        "Reason": fake.sentence(),
    }
    for i in range(1, NEW_ROWS + 1)
])

df_vitals = pd.DataFrame([
    {
        "VitalID": max_vital + i,
        "EncounterID": random.randint(1, max_encounter + NEW_ROWS),
        "Temperature": round(random.uniform(98.0, 102.0), 1),
        "Pulse": random.randint(60, 110),
        "BP": f"{random.randint(90,140)}/{random.randint(60,100)}",
        "RecordedAt": datetime.now(),
    }
    for i in range(1, NEW_ROWS + 1)
])

df_notes = pd.DataFrame([
    {
        "NoteID": max_note + i,
        "EncounterID": random.randint(1, max_encounter + NEW_ROWS),
        "NoteText": fake.text(max_nb_chars=200),
        "Author": fake.name(),
        "CreatedAt": datetime.now(),
    }
    for i in range(1, NEW_ROWS + 1)
])

# ================================
# INSERT INTO DB
# ================================
print(f"Inserting {NEW_ROWS} incremental rows into each table...")

df_patient.to_sql("Patient", engine, if_exists="append", index=False)
df_facility.to_sql("Facility", engine, if_exists="append", index=False)
df_encounter.to_sql("Encounter", engine, if_exists="append", index=False)
df_vitals.to_sql("Vitals", engine, if_exists="append", index=False)
df_notes.to_sql("ProgressNotes", engine, if_exists="append", index=False)

print("SUCCESS! Incremental data inserted.\n")
