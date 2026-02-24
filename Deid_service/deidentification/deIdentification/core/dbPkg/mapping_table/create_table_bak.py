import random
import datetime
import pandas as pd
import networkx as nx
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    BigInteger,
    Integer,
    DateTime,
)
from sqlalchemy.exc import ProgrammingError
import numpy as np
from core.dbPkg.mapping_loader import (
    PATIENT_MAPPING_TABLE,
    ENCOUNTER_MAPPING_TABLE,
    MAPPING_TABLE_CREATED_AT_COLUMN,
    MAPPING_TABLE_UPDATED_AT_COLUMN,
    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
    PATIENT_MAPPING_TABLE_OFFSET_COL,
    ENCOUNTER_MAPPING_TABLE_ENCID_COL,
    ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL,
)

from typing import TypedDict


class EncounterMappingConfig(TypedDict):
    table_name: str
    encounter_id_column: str
    patient_identifier_column: str
    patient_identifier_type: str
    encounter_date_column: str


class PatientMappingTableConfig(TypedDict):
    table_name: str
    columns: dict[str, str]  # {"PROFILEID": "profileid"}


class PatientMappingConfig(TypedDict):
    primary_id_column: str
    patient_identifier_columns: list[str]
    ndid_start_value: str
    tables: list[PatientMappingTableConfig]


class MappingConfig(TypedDict):
    connection_str: str
    patient_mapping_config: PatientMappingConfig
    encounter_mapping_config: EncounterMappingConfig


class MappingTable:
    def __init__(self, source_connection_str: str, config: MappingConfig):
        self.config = config
        self.patient_mapping_config = config["patient_mapping_config"]
        self.encounter_mapping_config = config["encounter_mapping_config"]
        self.source_engine = create_engine(source_connection_str)
        self.mapping_engine = create_engine(config["connection_str"])
        self.meta = MetaData()
        self.ndid_start_value = self.patient_mapping_config.get("ndid_start_value", 1)
        self.primary_id_column = config["patient_mapping_config"]["primary_id_column"]
        self.identifier_columns = config["patient_mapping_config"][
            "patient_identifier_columns"
        ]

    def get_sqlalchemy_column(self, name):
        if name == "nd_patient_id":
            return Column(name, BigInteger, nullable=False)
        elif name == "offset":
            return Column(name, Integer, nullable=False)
        elif name in {"created_at", "updated_at"}:
            return Column(name, DateTime, nullable=False)
        else:
            return Column(name, BigInteger, nullable=True)

    def get_all_patient_mapping_table_columns(self):
        output_columns = self.identifier_columns.copy()
        output_columns += [
            PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
            PATIENT_MAPPING_TABLE_OFFSET_COL,
            MAPPING_TABLE_CREATED_AT_COLUMN,
            MAPPING_TABLE_UPDATED_AT_COLUMN,
        ]
        return output_columns

    def create_patients_mapping_table(self):
        columns = [
            Column(name, BigInteger, nullable=True) for name in self.identifier_columns
        ]
        columns += [
            Column(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, BigInteger, nullable=False),
            Column(PATIENT_MAPPING_TABLE_OFFSET_COL, Integer, nullable=False),
            Column(MAPPING_TABLE_CREATED_AT_COLUMN, DateTime, nullable=False),
            Column(MAPPING_TABLE_UPDATED_AT_COLUMN, DateTime, nullable=False),
        ]
        table = Table(PATIENT_MAPPING_TABLE, self.meta, *columns)
        self.meta.create_all(self.mapping_engine)
        return table

    def fetch_and_normalize_dataframes(self, source_tables):
        dfs = []
        for table_cfg in source_tables:
            table_name = table_cfg["table_name"]
            columns = table_cfg["columns"]  # source_col: unified_col
            sql_cols = ", ".join(columns.values())
            renamed_columns = {v: k for k, v in columns.items()}
            df = pd.read_sql(f"SELECT {sql_cols} FROM {table_name}", self.source_engine)
            df.rename(columns=renamed_columns, inplace=True)
            for field in self.identifier_columns:
                if field not in df.columns:
                    df[field] = None
            df = df[self.identifier_columns]
            dfs.append(df)
        return dfs

    def build_unified_patient_mapping(self, dfs):
        G = nx.Graph()
        id_to_colval = dict()
        for df in dfs:
            for _, row in df.iterrows():
                ids_in_row = []
                for col in self.identifier_columns:
                    val = row[col]
                    if pd.notnull(val):
                        node_label = f"{col}:{val}"
                        ids_in_row.append(node_label)
                        id_to_colval[node_label] = (col, val)
                for i in range(len(ids_in_row)):
                    for j in range(i + 1, len(ids_in_row)):
                        G.add_edge(ids_in_row[i], ids_in_row[j])
        clusters = list(nx.connected_components(G))
        unified_rows = []
        ndid_start = self.ndid_start_value
        for idx, cluster in enumerate(clusters):
            row_dict = {col: None for col in self.identifier_columns}
            for node in cluster:
                col, val = id_to_colval[node]
                if row_dict[col] is None:
                    row_dict[col] = val
            row_dict["nd_patient_id"] = ndid_start + idx
            row_dict["offset"] = random.randint(10, 30)
            now = datetime.datetime.utcnow()
            row_dict["created_at"] = now
            row_dict["updated_at"] = now
            unified_rows.append(row_dict)
        df_result = pd.DataFrame(unified_rows)
        for col in self.get_all_patient_mapping_table_columns():
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[self.get_all_patient_mapping_table_columns()]
        return df_result

    def generate_patient_mapping_table(self):
        self.create_patients_mapping_table()
        dfs = self.fetch_and_normalize_dataframes(self.patient_mapping_config["tables"])
        full_df = self.build_unified_patient_mapping(dfs)
        full_df.to_sql(
            PATIENT_MAPPING_TABLE,
            con=self.mapping_engine,
            if_exists="replace",
            index=False,
            chunksize=5000,
            method="multi",
        )
        print(f"Inserted {len(full_df)} records into {PATIENT_MAPPING_TABLE}.")

    def update_patient_mapping_table(self, new_source_tables=None):
        try:
            existing_df = pd.read_sql(
                f"SELECT * FROM {PATIENT_MAPPING_TABLE}", self.mapping_engine
            )
        except ProgrammingError:
            print(f"{PATIENT_MAPPING_TABLE} does not exist. Run generate_patient_mapping_table() first.")
            return

        source_tables = (
            new_source_tables if new_source_tables else self.patient_mapping_config["tables"]
        )
        dfs = self.fetch_and_normalize_dataframes(source_tables)
        new_full_df = self.build_unified_patient_mapping(dfs)

        if not existing_df.empty:
            mask = ~new_full_df[self.primary_id_column].isin(existing_df[self.primary_id_column])
            merged_df = new_full_df[mask]
        else:
            merged_df = new_full_df

        if merged_df.empty:
            print("No new data to update.")
            return

        now = datetime.datetime.utcnow()
        merged_df["created_at"] = now
        merged_df["updated_at"] = now
        merged_df["offset"] = [random.randint(10, 30) for _ in range(len(merged_df))]
        merged_df = merged_df[self.get_all_patient_mapping_table_columns()]
        final_df = pd.concat([existing_df, merged_df], ignore_index=True)
        final_df = final_df[self.get_all_patient_mapping_table_columns()]
        final_df.to_sql(
            PATIENT_MAPPING_TABLE,
            con=self.mapping_engine,
            # if_exists="append",
            if_exists="replace",
            index=False,
            chunksize=5000,
            method="multi",
        )
        print(f"Updated {PATIENT_MAPPING_TABLE}: {len(merged_df)} new records added.")

    def generate_encounter_mapping_table(self):
        # 1. Load patient mapping: get nd_patient_id for merge
        patient_map = pd.read_sql(
            f"SELECT {self.encounter_mapping_config['patient_identifier_type']}, {PATIENT_MAPPING_TABLE_ND_PATIENTID_COL} "
            f"FROM {PATIENT_MAPPING_TABLE}",
            self.mapping_engine
        )

        # 2. Load existing unified encounters (may be empty)
        try:
            existing_enc = pd.read_sql(
                f"SELECT {PATIENT_MAPPING_TABLE_ND_PATIENTID_COL}, {ENCOUNTER_MAPPING_TABLE_ENCID_COL}, "
                f"{MAPPING_TABLE_CREATED_AT_COLUMN}, {MAPPING_TABLE_UPDATED_AT_COLUMN}"
                f"FROM {ENCOUNTER_MAPPING_TABLE}",
                self.mapping_engine,
            )
        except Exception:
            existing_enc = pd.DataFrame(
                columns=[
                    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
                    ENCOUNTER_MAPPING_TABLE_ENCID_COL,
                    MAPPING_TABLE_CREATED_AT_COLUMN,
                    MAPPING_TABLE_UPDATED_AT_COLUMN,
                ]
            )

        # 3. Load new (possibly incremental) encounters
        encounters_cols = [
            self.encounter_mapping_config["encounter_id_column"],
            self.encounter_mapping_config["patient_identifier_column"]
        ]
        if "encounter_date_column" in self.encounter_mapping_config:
            encounters_cols.append(self.encounter_mapping_config["encounter_date_column"])
        enc_df = pd.read_sql(
            f"SELECT {', '.join(encounters_cols)} FROM {self.encounter_mapping_config['table_name']}",
            self.source_engine,
        )
        enc_df.rename(
            columns={
                self.encounter_mapping_config["patient_identifier_column"]: self.encounter_mapping_config["patient_identifier_type"],
                self.encounter_mapping_config["encounter_id_column"]: ENCOUNTER_MAPPING_TABLE_ENCID_COL,
                self.encounter_mapping_config.get("encounter_date_column", ""): "registration_date",
            },
            inplace=True,
        )

        # 4. MAP TO ND_PATIENT_ID for new data
        enc_df = enc_df.merge(
            patient_map,
            how="left",
            on=self.encounter_mapping_config["patient_identifier_type"],
            validate="many_to_one"
        )
        enc_df = enc_df[enc_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL].notnull()].copy()

        if not existing_enc.empty:
            prev_keys = set(
                tuple(x)
                for x in existing_enc[
                    [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, ENCOUNTER_MAPPING_TABLE_ENCID_COL]
                ]
                .astype(str)
                .values
            )
            enc_df = enc_df[
                ~enc_df[
                    [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, ENCOUNTER_MAPPING_TABLE_ENCID_COL]
                ]
                .astype(str)
                .apply(tuple, axis=1)
                .isin(prev_keys)
            ].copy()
        if enc_df.empty:
            print("No new encounters to process.")
            return

        # 6. Registration date column (ensure present in both frames)
        if "registration_date" in enc_df.columns and "registration_date" not in existing_enc.columns:
            existing_enc["registration_date"] = pd.NaT
        if "registration_date" in existing_enc.columns and "registration_date" not in enc_df.columns:
            enc_df["registration_date"] = pd.NaT

        # 7. Sort by patient and encounter/date
        sort_cols = [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL]
        if "registration_date" in enc_df.columns:
            sort_cols.append("registration_date")
        sort_cols.append(ENCOUNTER_MAPPING_TABLE_ENCID_COL)

        existing_enc = existing_enc.sort_values(by=sort_cols)
        enc_df = enc_df.sort_values(by=sort_cols)

        existing_enc = existing_enc.assign(source="old")
        enc_df = enc_df.assign(source="new")

        both = pd.concat([existing_enc, enc_df], ignore_index=True, sort=False)

        if MAPPING_TABLE_CREATED_AT_COLUMN not in both.columns:
            both[MAPPING_TABLE_CREATED_AT_COLUMN] = pd.NaT
        if MAPPING_TABLE_UPDATED_AT_COLUMN not in both.columns:
            both[MAPPING_TABLE_UPDATED_AT_COLUMN] = pd.NaT

        # 8. Order for counter: by nd_patient_id
        sort_cols2 = [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL]
        if "registration_date" in both.columns:
            sort_cols2.append("registration_date")
        sort_cols2.append(ENCOUNTER_MAPPING_TABLE_ENCID_COL)
        if "source" in both.columns:
            both["source"] = pd.Categorical(both["source"], categories=["old", "new"], ordered=True)
            sort_cols2.append("source")
        both = both.sort_values(by=sort_cols2).reset_index(drop=True)

        # 9. Assign counter and nd_encounter_id (PATIENT_MAPPING_TABLE_ND_PATIENTID_COL is now the patient)
        both["encid_counter"] = both.groupby(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL).cumcount() + 1
        both[ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL] = both.apply(
            lambda row: f"{int(row[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL])}{int(row['encid_counter']):04d}",
            axis=1
        )
        now = datetime.datetime.utcnow()
        both[MAPPING_TABLE_CREATED_AT_COLUMN] = both[MAPPING_TABLE_CREATED_AT_COLUMN].fillna(now)
        both[MAPPING_TABLE_UPDATED_AT_COLUMN] = both[MAPPING_TABLE_UPDATED_AT_COLUMN].fillna(now)

        # 10. Only keep new rows, and only nd_patient_id as the patient identifier reference in output
        output_cols = [
            PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
            ENCOUNTER_MAPPING_TABLE_ENCID_COL,
            ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL,
            MAPPING_TABLE_CREATED_AT_COLUMN,
            MAPPING_TABLE_UPDATED_AT_COLUMN,
        ]
        only_new = both[both["source"] == "new"][output_cols]
        if not only_new.empty:
            only_new.to_sql(
                ENCOUNTER_MAPPING_TABLE,
                con=self.mapping_engine,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi",
            )
            print(
                f"Appended {len(only_new)} new encounters to {ENCOUNTER_MAPPING_TABLE}."
            )
        else:
            print("No new encounters to update.")
