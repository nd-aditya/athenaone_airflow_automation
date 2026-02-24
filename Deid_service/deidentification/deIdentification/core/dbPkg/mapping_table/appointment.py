import random
import datetime
import pandas as pd
import networkx as nx
from sqlalchemy import (
    create_engine,
    MetaData,
    text
)
from core.dbPkg.mapping_loader import (
    PATIENT_MAPPING_TABLE,
    MAPPING_TABLE_CREATED_AT_COLUMN,
    MAPPING_TABLE_UPDATED_AT_COLUMN,
    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
)
from deIdentification.nd_logger import nd_logger
from typing import TypedDict


APPOINTMENT_MAPPING_TABLE = "appointment_mapping_table"
APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL = "appointment_id"
APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL = "appointment_date"
APPOINTMENT_MAPPING_TABLE_ND_APPOINTMENT_ID_COL = "nd_appointment_id"

class AppointmentMappingConfig(TypedDict):
    table_name: str
    appointment_id_column: str
    patient_identifier_column: str
    patient_identifier_type: str
    appointment_date_column: str




class AppointmentMappingTable:
    def __init__(self, source_connection_str: str, config: AppointmentMappingConfig, client_id: int, dump_id: int):
        self.config = config
        self.client_id = client_id
        self.dump_id = dump_id
        self.source_connection_str = source_connection_str
        self.patient_mapping_config = config["patient_mapping_config"]
        self.appointment_mapping_config = config
        self.source_engine = create_engine(source_connection_str)
        self.appointment_engine = create_engine(config["connection_str"])
        self.meta = MetaData()

    def backup_table(self, table_name: str):
        backup_table_name = f"{table_name}_nd_backup_client{self.client_id}_dump{self.dump_id}"
        with self.appointment_engine.connect() as conn:
            check_sql = text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = :table_name"
            )
            result = conn.execute(check_sql, {"table_name": table_name}).scalar()
            if result == 0:
                nd_logger.info(f"Table {table_name} does not exist. Skipping backup.")
                return
            drop_sql = text(f"DROP TABLE IF EXISTS {backup_table_name}")
            conn.execute(drop_sql)

            create_sql = text(f"CREATE TABLE {backup_table_name} AS SELECT * FROM {table_name};")
            conn.execute(create_sql)
            nd_logger.info(
                f"Client: {self.client_id}, Dump: {self.dump_id} - "
                f"Backup table recreated: {backup_table_name}"
            )

    def generate_appointment_mapping_table(self):
        self.backup_table(APPOINTMENT_MAPPING_TABLE)
        # 1. Load patient mapping: get nd_patient_id for merge
        patient_map = pd.read_sql(
            f"SELECT {self.appointment_mapping_config['patient_identifier_type']}, {PATIENT_MAPPING_TABLE_ND_PATIENTID_COL} "
            f"FROM {PATIENT_MAPPING_TABLE}",
            self.appointment_engine
        )

        # 2. Load existing unified encounters (may be empty)
        try:
            existing_appointments = pd.read_sql(
                f"SELECT {PATIENT_MAPPING_TABLE_ND_PATIENTID_COL}, {APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL}, "
                f"{APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL}, "
                f"{MAPPING_TABLE_CREATED_AT_COLUMN}, {MAPPING_TABLE_UPDATED_AT_COLUMN}"
                f"FROM {APPOINTMENT_MAPPING_TABLE}",
                self.appointment_engine,
            )
        except Exception:
            existing_appointments = pd.DataFrame(
                columns=[
                    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
                    APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL,
                    APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL,
                    MAPPING_TABLE_CREATED_AT_COLUMN,
                    MAPPING_TABLE_UPDATED_AT_COLUMN,
                ]
            )
        # 3. Load new (possibly incremental) encounters
        appointments_cols = [
            self.appointment_mapping_config["appointment_id_column"],
            self.appointment_mapping_config["patient_identifier_column"],
            self.appointment_mapping_config["appointment_date_column"]
        ]
        appointment_df = pd.read_sql(
            f"SELECT {', '.join(appointments_cols)} FROM {self.appointment_mapping_config['table_name']}",
            self.source_engine,
        )
        appointment_df.rename(
            columns={
                self.appointment_mapping_config["patient_identifier_column"]: self.appointment_mapping_config["patient_identifier_type"],
                self.appointment_mapping_config["appointment_id_column"]: APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL,
                self.appointment_mapping_config.get("appointment_date_column", ""): APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL,
            },
            inplace=True,
        )

        # 4. MAP TO ND_PATIENT_ID for new data
        # breakpoint()
        # patient_map = patient_map.drop_duplicates(subset=["chartid"], keep="first")
        appointment_df = appointment_df.merge(
            patient_map,
            how="left",
            on=self.appointment_mapping_config["patient_identifier_type"],
            validate="many_to_one"
        )
        appointment_df = appointment_df[appointment_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL].notnull()].copy()

        if not existing_appointments.empty:
            prev_keys = set(
                tuple(x)
                for x in existing_appointments[
                    [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL]
                ]
                .astype(str)
                .values
            )
            appointment_df = appointment_df[
                ~appointment_df[
                    [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL]
                ]
                .astype(str)
                .apply(tuple, axis=1)
                .isin(prev_keys)
            ].copy()
        if appointment_df.empty:
            print("No new encounters to process.")
            return

        # 6. Registration date column (ensure present in both frames)
        if APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL in appointment_df.columns and APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL not in existing_appointments.columns:
            appointment_df[APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL] = pd.NaT
        if APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL in existing_appointments.columns and APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL not in appointment_df.columns:
            appointment_df[APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL] = pd.NaT

        # 7. Sort by patient and encounter/date
        sort_cols = [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL]
        if APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL in appointment_df.columns:
            sort_cols.append(APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL)
        sort_cols.append(APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL)

        existing_enc = existing_appointments.sort_values(by=sort_cols)
        enc_df = appointment_df.sort_values(by=sort_cols)

        existing_enc = existing_enc.assign(source="old")
        enc_df = enc_df.assign(source="new")

        both = pd.concat([existing_enc, enc_df], ignore_index=True, sort=False)

        if MAPPING_TABLE_CREATED_AT_COLUMN not in both.columns:
            both[MAPPING_TABLE_CREATED_AT_COLUMN] = pd.NaT
        if MAPPING_TABLE_UPDATED_AT_COLUMN not in both.columns:
            both[MAPPING_TABLE_UPDATED_AT_COLUMN] = pd.NaT

        # 8. Order for counter: by nd_patient_id
        sort_cols2 = [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL]
        if APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL in both.columns:
            sort_cols2.append(APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL)
        sort_cols2.append(APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL)
        if "source" in both.columns:
            both["source"] = pd.Categorical(both["source"], categories=["old", "new"], ordered=True)
            sort_cols2.append("source")
        both = both.sort_values(by=sort_cols2).reset_index(drop=True)

        # 9. Assign counter and nd_encounter_id (PATIENT_MAPPING_TABLE_ND_PATIENTID_COL is now the patient)
        both["appointment_counter"] = both.groupby(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL).cumcount() + 1
        both[APPOINTMENT_MAPPING_TABLE_ND_APPOINTMENT_ID_COL] = both.apply(
            lambda row: f"{int(row[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL])}{int(row['appointment_counter']):03d}",
            axis=1
        )
        now = datetime.datetime.utcnow()
        both[MAPPING_TABLE_CREATED_AT_COLUMN] = both[MAPPING_TABLE_CREATED_AT_COLUMN].fillna(now)
        both[MAPPING_TABLE_UPDATED_AT_COLUMN] = both[MAPPING_TABLE_UPDATED_AT_COLUMN].fillna(now)

        # 10. Only keep new rows, and only nd_patient_id as the patient identifier reference in output
        output_cols = [
            PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
            APPOINTMENT_MAPPING_TABLE_APPOINTMENT_ID_COL,
            APPOINTMENT_MAPPING_TABLE_ND_APPOINTMENT_ID_COL,
            APPOINTMENT_MAPPING_TABLE_APPOINTMENT_DATE_COL,
            MAPPING_TABLE_CREATED_AT_COLUMN,
            MAPPING_TABLE_UPDATED_AT_COLUMN,
        ]
        only_new = both[both["source"] == "new"][output_cols]
        if not only_new.empty:
            only_new.to_sql(
                APPOINTMENT_MAPPING_TABLE,
                con=self.appointment_engine,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi",
            )
            nd_logger.info(
                f"Client: {self.client_id}, Dump: {self.dump_id} - Appended {len(only_new)} new encounters to {APPOINTMENT_MAPPING_TABLE}."
            )
        else:
            nd_logger.info(f"Client: {self.client_id}, Dump: {self.dump_id} - No new appointments to update.")
