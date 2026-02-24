import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine

CONNECTION_STRING = "mysql+pymysql://root:123456789@localhost/dummy_mapping_schema"

N_ENCOUNTERS = 100             # Number of encounters to create
N_PATIENTS = 4               # Number of distinct patient_ids
START_DATE = datetime(2023, 1, 1)
ENC_START_VALUE = 100     # Your custom starting encounter id

def generate_dummy_encounters(n_encs, n_patients, start_date, enc_start_value):
    patient_ids = np.random.choice(range(1000, 1000 + n_patients), n_encs)
    enc_ids = np.arange(enc_start_value, enc_start_value + n_encs)
    registration_dates = [start_date + timedelta(days=int(i)) for i in np.random.randint(0, 365, n_encs)]
    encounters = pd.DataFrame({
        "enc_id": enc_ids,                     
        "patient_id": patient_ids,             
        "registration_date": registration_dates
    })
    return encounters

def main():
    engine = create_engine(CONNECTION_STRING)
    df = generate_dummy_encounters(N_ENCOUNTERS, N_PATIENTS, START_DATE, ENC_START_VALUE)
    df.to_sql("encounters", engine, index=False, if_exists='replace')
    print("Dummy 'encounters' table created and populated.")

if __name__ == "__main__":
    main()
