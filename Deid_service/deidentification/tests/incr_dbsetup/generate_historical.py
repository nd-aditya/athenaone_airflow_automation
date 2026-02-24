from sqlalchemy import create_engine, text
import pandas as pd
from faker import Faker
import random
from datetime import datetime
import urllib

# ================================
# CONFIG
# ================================
SERVER = "localhost:1433"
DATABASE = "automation_incr_testing"
USERNAME = "sa"
PASSWORD = "ndAdmin2025"
ROWS = 100

fake = Faker()

# SQLAlchemy connection using ODBC
connection_str = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
engine = create_engine(connection_str)

# ================================
# CREATE TABLES
# ================================
print("Dropping & creating tables...")

ddl = """
IF OBJECT_ID('Patient', 'U') IS NOT NULL DROP TABLE Patient;
CREATE TABLE Patient (
    PatientID INT PRIMARY KEY,
    Name NVARCHAR(100),
    Gender VARCHAR(10),
    DOB DATE,
    CreatedAt DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('Facility', 'U') IS NOT NULL DROP TABLE Facility;
CREATE TABLE Facility (
    FacilityID INT PRIMARY KEY,
    Name NVARCHAR(100),
    City NVARCHAR(50),
    State NVARCHAR(50),
    CreatedAt DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('Encounter', 'U') IS NOT NULL DROP TABLE Encounter;
CREATE TABLE Encounter (
    EncounterID INT PRIMARY KEY,
    PatientID INT,
    FacilityID INT,
    EncounterDate DATETIME,
    Reason NVARCHAR(255),
    FOREIGN KEY (PatientID) REFERENCES Patient(PatientID),
    FOREIGN KEY (FacilityID) REFERENCES Facility(FacilityID)
);

IF OBJECT_ID('Vitals', 'U') IS NOT NULL DROP TABLE Vitals;
CREATE TABLE Vitals (
    VitalID INT PRIMARY KEY,
    EncounterID INT,
    Temperature FLOAT,
    Pulse INT,
    BP VARCHAR(20),
    RecordedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (EncounterID) REFERENCES Encounter(EncounterID)
);

IF OBJECT_ID('ProgressNotes', 'U') IS NOT NULL DROP TABLE ProgressNotes;
CREATE TABLE ProgressNotes (
    NoteID INT PRIMARY KEY,
    EncounterID INT,
    NoteText NVARCHAR(MAX),
    Author NVARCHAR(100),
    CreatedAt DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (EncounterID) REFERENCES Encounter(EncounterID)
);
"""

with engine.begin() as conn:
    conn.execute(text(ddl))

print("Tables created successfully!")

# ================================
# GENERATE DATA USING PANDAS + FAKER
# ================================
print("Generating dummy data...")

df_patient = pd.DataFrame([
    {
        "PatientID": i,
        "Name": fake.name(),
        "Gender": random.choice(["Male", "Female"]),
        "DOB": fake.date_of_birth(),
        "CreatedAt": datetime.now(),
    }
    for i in range(1, ROWS + 1)
])

df_facility = pd.DataFrame([
    {
        "FacilityID": i,
        "Name": fake.company(),
        "City": fake.city(),
        "State": fake.state(),
        "CreatedAt": datetime.now(),
    }
    for i in range(1, ROWS + 1)
])

df_encounter = pd.DataFrame([
    {
        "EncounterID": i,
        "PatientID": random.randint(1, ROWS),
        "FacilityID": random.randint(1, ROWS),
        "EncounterDate": fake.date_time_between(start_date="-2y", end_date="now"),
        "Reason": fake.sentence(),
    }
    for i in range(1, ROWS + 1)
])

df_vitals = pd.DataFrame([
    {
        "VitalID": i,
        "EncounterID": random.randint(1, ROWS),
        "Temperature": round(random.uniform(97.5, 103.0), 1),
        "Pulse": random.randint(60, 110),
        "BP": f"{random.randint(90,140)}/{random.randint(60,100)}",
        "RecordedAt": datetime.now(),
    }
    for i in range(1, ROWS + 1)
])

df_notes = pd.DataFrame([
    {
        "NoteID": i,
        "EncounterID": random.randint(1, ROWS),
        "NoteText": fake.text(max_nb_chars=200),
        "Author": fake.name(),
        "CreatedAt": datetime.now(),
    }
    for i in range(1, ROWS + 1)
])

# ================================
# INSERT DATA USING to_sql()
# ================================
print("Inserting data using pandas + SQLAlchemy...")

df_patient.to_sql("Patient", engine, if_exists="append", index=False)
df_facility.to_sql("Facility", engine, if_exists="append", index=False)
df_encounter.to_sql("Encounter", engine, if_exists="append", index=False)
df_vitals.to_sql("Vitals", engine, if_exists="append", index=False)
df_notes.to_sql("ProgressNotes", engine, if_exists="append", index=False)

print("DONE! All 5 tables loaded with 100 records each.")
