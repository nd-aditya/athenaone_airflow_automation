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
    text
)
from sqlalchemy.exc import ProgrammingError
import numpy as np
from core.dbPkg.mapping_loader import (
    PATIENT_MAPPING_TABLE,
    ENCOUNTER_MAPPING_TABLE,
    MAPPING_TABLE_CREATED_AT_COLUMN,
    MAPPING_TABLE_UPDATED_AT_COLUMN,
    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
    PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL,
    ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL,
    PATIENT_MAPPING_TABLE_OFFSET_COL,
    ENCOUNTER_MAPPING_TABLE_ENCID_COL,
    ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL,
)
from deIdentification.nd_logger import nd_logger
from typing import TypedDict

# #region agent log
def _debug_log(location: str, message: str, data: dict, hypothesis_id: str = ""):
    import json
    import os
    try:
        log_dir = os.path.join(os.getcwd(), ".cursor")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "debug-2ecf71.log")
        with open(log_path, "a") as f:
            f.write(json.dumps({"sessionId": "2ecf71", "location": location, "message": message, "data": data, "hypothesisId": hypothesis_id, "timestamp": datetime.datetime.utcnow().isoformat()}) + "\n")
    except Exception:
        pass
# #endregion


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
    def __init__(self, source_connection_str: str, config: MappingConfig, queue_id: int, client_type: str):
        self.config = config
        self.queue_id = queue_id
        self.client_type = client_type
        self.source_connection_str = source_connection_str
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
        elif name == PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL:
            return Column(name, DateTime, nullable=True)
        else:
            return Column(name, BigInteger, nullable=True)
    
    def backup_table(self, table_name: str):
        backup_table_name = f"{table_name}_nd_backup_queue{self.queue_id}"
        with self.mapping_engine.connect() as conn:
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
                f"Queue: {self.queue_id} - "
                f"Backup table recreated: {backup_table_name}"
            )


    def get_all_patient_mapping_table_columns(self):
        output_columns = self.identifier_columns.copy()
        output_columns += [
            PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
            PATIENT_MAPPING_TABLE_OFFSET_COL,
            PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL,
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
            Column(PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL, DateTime, nullable=True),
            Column(MAPPING_TABLE_CREATED_AT_COLUMN, DateTime, nullable=False),
            Column(MAPPING_TABLE_UPDATED_AT_COLUMN, DateTime, nullable=False),
        ]
        table = Table(PATIENT_MAPPING_TABLE, self.meta, *columns)
        self.meta.create_all(self.mapping_engine)
        return table

    def fetch_and_normalize_dataframes(self, source_tables):
        dfs = []
        for table_cfg in source_tables:
            # table_cfg = {'columns': {'chartid': 'chartid', 'patientid': 'patient_id'}, 'table_name': 'table1', 'registration_date': 'registration_date'}
            table_name = table_cfg["table_name"]
            registration_date = table_cfg.get("registration_date", "created_date")
            columns = table_cfg["columns"]
            sql_cols = ", ".join(columns.values())
            renamed_columns = {v: k for k, v in columns.items()}
            if self.client_type.lower() == "ecw":
                df = pd.read_sql(f"SELECT {sql_cols}, {registration_date} FROM {table_name} where UserType = 3", self.source_engine)
            else:
                df = pd.read_sql(f"SELECT {sql_cols}, {registration_date} FROM {table_name}", self.source_engine)
            df.rename(columns=renamed_columns, inplace=True)
            df.rename(columns={registration_date: PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL}, inplace=True)
            for field in self.identifier_columns:
                if field not in df.columns:
                    df[field] = None
            if PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL in df.columns:
                df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL] = pd.to_datetime(df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL], errors='coerce')
            df = df[self.identifier_columns + [PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL]]
            dfs.append(df)
        return dfs

    def _normalize_value(self, value):
        """
        Normalize a value for comparison. Converts numeric strings to numbers
        so that "202", "202.0", "202.0000" are treated as the same.
        """
        if pd.isna(value):
            return None
        try:
            # Try to convert to float first (handles both int and float strings)
            num_val = float(value)
            # If it's a whole number, return as int for consistency
            if num_val.is_integer():
                return int(num_val)
            return num_val
        except (ValueError, TypeError):
            # If conversion fails, return as string
            return str(value)
    
    def filter_existing_patients(self, dfs, existing_df):
        """
        Filter out rows from dfs that match existing patients.
        A row matches if any of its non-null identifier values exist in existing_df.
        """
        if existing_df.empty:
            return dfs
        
        # Create sets of existing identifier values for fast lookup
        existing_identifier_sets = {}
        for col in self.identifier_columns:
            if col in existing_df.columns:
                # Get non-null values, normalize them, and convert to set for fast lookup
                normalized_values = existing_df[col].dropna().apply(self._normalize_value)
                existing_identifier_sets[col] = set(normalized_values)
            else:
                existing_identifier_sets[col] = set()
        
        filtered_dfs = []
        for df in dfs:
            # Create a mask to identify rows that match existing patients
            # A row matches if ANY of its non-null identifier values exist in existing_df
            mask = pd.Series([False] * len(df), index=df.index)
            for col in [self.primary_id_column]:
                if col in df.columns:
                # if col in [self.primary_id_column]:
                    # Normalize values and check if any value in this column matches existing patients
                    normalized_col = df[col].apply(self._normalize_value)
                    col_mask = df[col].notna() & normalized_col.isin(existing_identifier_sets[col])
                    mask = mask | col_mask
            
            # Keep only rows that don't match existing patients
            filtered_df = df[~mask].copy()
            filtered_dfs.append(filtered_df)
        
        return filtered_dfs

    
    def build_unified_patient_mapping(self, dfs, ndid_start=None):
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
                        id_to_colval[node_label] = (col, val, row[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL])
                
                # Add edges only if there are 2+ identifiers in the row
                if len(ids_in_row) >= 2:
                    for i in range(len(ids_in_row)):
                        for j in range(i + 1, len(ids_in_row)):
                            G.add_edge(ids_in_row[i], ids_in_row[j])
                else:
                    # For single identifiers, add as isolated nodes
                    for node in ids_in_row:
                        G.add_node(node)
        
        # Get connected components (includes isolated nodes)
        clusters = list(nx.connected_components(G))
        
        unified_rows = []
        if ndid_start is None:
            ndid_start = self.ndid_start_value
        for idx, cluster in enumerate(clusters):
            row_dict = {col: None for col in self.identifier_columns}
            registration_date = None
            for node in cluster:
                col, val, registration_date = id_to_colval[node]
                if row_dict[col] is None:
                    row_dict[col] = val
                if registration_date is None:
                    registration_date = registration_date
            row_dict["nd_patient_id"] = ndid_start + idx
            row_dict["offset"] = random.randint(10, 30)
            now = datetime.datetime.utcnow()
            row_dict["registration_date"] = registration_date
            row_dict["created_at"] = now
            row_dict["updated_at"] = now
            unified_rows.append(row_dict)
        
        df_result = pd.DataFrame(unified_rows)
        # Ensure registration_date is datetime
        if PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL in df_result.columns:
            df_result[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL] = pd.to_datetime(df_result[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL], errors='coerce')
        for col in self.get_all_patient_mapping_table_columns():
            if col not in df_result.columns:
                df_result[col] = None
        df_result = df_result[self.get_all_patient_mapping_table_columns()]
        return df_result


    def generate_patient_mapping_table(self):
        # self.backup_table(PATIENT_MAPPING_TABLE)

        self.create_patients_mapping_table()
        dfs = self.fetch_and_normalize_dataframes(self.patient_mapping_config["tables"])
        full_df = self.build_unified_patient_mapping(dfs)
        # Ensure registration_date is datetime before writing to SQL
        if PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL in full_df.columns:
            full_df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL] = pd.to_datetime(full_df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL], errors='coerce')
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
        self.backup_table(PATIENT_MAPPING_TABLE)
        try:
            existing_df = pd.read_sql(
                f"SELECT * FROM {PATIENT_MAPPING_TABLE}", self.mapping_engine
            )
        except ProgrammingError:
            print(f"{PATIENT_MAPPING_TABLE} does not exist. Run generate_patient_mapping_table() first.")
            return

        # Calculate the max nd_patient_id from existing data to continue from where it left off
        if not existing_df.empty and PATIENT_MAPPING_TABLE_ND_PATIENTID_COL in existing_df.columns:
            max_nd_patient_id = existing_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL].max()
            ndid_start = int(max_nd_patient_id) + 1
        else:
            ndid_start = self.ndid_start_value

        source_tables = (
            new_source_tables if new_source_tables else self.patient_mapping_config["tables"]
        )
        # #region agent log
        _table_names = [t.get("table_name", t) for t in source_tables] if source_tables else []
        _debug_log("create_table.py:update_patient_mapping_table", "source_tables for patient mapping", {"count": len(source_tables), "table_names": _table_names, "queue_id": self.queue_id, "from_queue": new_source_tables is not None}, "H3")
        # #endregion
        # Fetch all data from source tables
        dfs = self.fetch_and_normalize_dataframes(source_tables)
        
        # Filter out existing patients BEFORE building unified mapping
        # This ensures we only assign nd_patient_id to truly new patients
        filtered_dfs = self.filter_existing_patients(dfs, existing_df)
        
        # Check if there are any new patients to process
        if all(df.empty for df in filtered_dfs):
            print("No new data to update.")
            return
        
        # Build unified mapping only for new patients
        merged_df = self.build_unified_patient_mapping(filtered_dfs, ndid_start=ndid_start)

        # merged_df already has created_at, updated_at, and offset from build_unified_patient_mapping
        # Update timestamps to reflect the update operation
        now = datetime.datetime.utcnow()
        merged_df["created_at"] = now
        merged_df["updated_at"] = now
        # Offset is already set in build_unified_patient_mapping, but we can regenerate if needed
        merged_df["offset"] = [random.randint(10, 30) for _ in range(len(merged_df))]
        # Ensure registration_date is datetime before writing to SQL
        if PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL in merged_df.columns:
            merged_df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL] = pd.to_datetime(merged_df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL], errors='coerce')
        merged_df = merged_df[self.get_all_patient_mapping_table_columns()]
        final_df = pd.concat([existing_df, merged_df], ignore_index=True)
        final_df = final_df[self.get_all_patient_mapping_table_columns()]
        # Ensure registration_date is datetime before writing to SQL
        if PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL in final_df.columns:
            final_df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL] = pd.to_datetime(final_df[PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL], errors='coerce')
        # #region agent log
        _prim = self.identifier_columns[0] if self.identifier_columns else (list(final_df.columns)[0] if not final_df.empty else None)
        _final_dup = int(final_df.duplicated(subset=[_prim], keep=False).sum()) if _prim and _prim in final_df.columns else 0
        _merged_dup = int(merged_df.duplicated(subset=[_prim], keep=False).sum()) if _prim and _prim in merged_df.columns else 0
        _debug_log("create_table.py:update_patient_mapping_table", "before to_sql: final_df and merged_df duplicate counts", {"final_df_duplicate_count": _final_dup, "merged_df_duplicate_count": _merged_dup, "primary_id_col": _prim, "queue_id": self.queue_id}, "H4")
        # #endregion
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
        self.backup_table(ENCOUNTER_MAPPING_TABLE)
        # 1. Load patient mapping: get nd_patient_id for merge
        patient_map = pd.read_sql(
            f"SELECT {self.encounter_mapping_config['patient_identifier_type']}, {PATIENT_MAPPING_TABLE_ND_PATIENTID_COL} "
            f"FROM {PATIENT_MAPPING_TABLE}",
            self.mapping_engine
        )
        # #region agent log
        _join_key = self.encounter_mapping_config["patient_identifier_type"]
        _dup = patient_map.duplicated(subset=[_join_key], keep=False)
        _dup_count = int(_dup.sum())
        _dup_keys = patient_map.loc[_dup, _join_key].drop_duplicates().head(5).tolist() if _dup_count else []
        _debug_log("create_table.py:generate_encounter_mapping_table", "patient_map loaded from PATIENT_MAPPING_TABLE", {"shape": list(patient_map.shape), "join_key": _join_key, "duplicate_count": _dup_count, "sample_duplicate_keys": _dup_keys, "queue_id": self.queue_id}, "H1")
        # #endregion

        # 2. Load existing unified encounters (may be empty)
        try:
            existing_enc = pd.read_sql(
                f"SELECT {PATIENT_MAPPING_TABLE_ND_PATIENTID_COL}, {ENCOUNTER_MAPPING_TABLE_ENCID_COL}, "
                f"{ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL}, "
                f"{ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL}, "
                f"{MAPPING_TABLE_CREATED_AT_COLUMN}, {MAPPING_TABLE_UPDATED_AT_COLUMN} "
                f"FROM {ENCOUNTER_MAPPING_TABLE}",
                self.mapping_engine,
            )
        except Exception:
            existing_enc = pd.DataFrame(
                columns=[
                    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
                    ENCOUNTER_MAPPING_TABLE_ENCID_COL,
                    ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL,
                    ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL,
                    MAPPING_TABLE_CREATED_AT_COLUMN,
                    MAPPING_TABLE_UPDATED_AT_COLUMN,
                ]
            )

        # 3. Load new (possibly incremental) encounters
        encounters_cols = [
            self.encounter_mapping_config["encounter_id_column"],
            self.encounter_mapping_config["patient_identifier_column"],
            self.encounter_mapping_config["encounter_date_column"]
        ]
        enc_df = pd.read_sql(
            f"SELECT {', '.join(encounters_cols)} FROM {self.encounter_mapping_config['table_name']}",
            self.source_engine,
        )
        enc_df.rename(
            columns={
                self.encounter_mapping_config["patient_identifier_column"]: self.encounter_mapping_config["patient_identifier_type"],
                self.encounter_mapping_config["encounter_id_column"]: ENCOUNTER_MAPPING_TABLE_ENCID_COL,
                self.encounter_mapping_config["encounter_date_column"]: ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL,
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

        # --- Calculate max_counter BEFORE removing existing encounter ids from enc_df ---
        if not existing_enc.empty:
            # Always recalculate encid_counter for existing_enc by grouping by nd_patient_id and counting
            existing_enc = existing_enc.copy()
            # Sort by patient and encounter_id to ensure consistent ordering
            existing_enc = existing_enc.sort_values(
                by=[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, ENCOUNTER_MAPPING_TABLE_ENCID_COL]
            ).reset_index(drop=True)
            # Assign encid_counter as a sequential count per patient (starting from 1)
            existing_enc["encid_counter"] = existing_enc.groupby(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL).cumcount() + 1
            # Now get the max_counter per patient
            max_counter = existing_enc.groupby(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL)["encid_counter"].max()
        else:
            max_counter = pd.Series(dtype=int)
        # --- End max_counter calculation ---

        # Remove new encounters that already exist in the mapping table (by encounter_id only)
        if not existing_enc.empty:
            # Consider encounter_id as unique: remove any rows from enc_df whose encounter_id is already present
            existing_encounter_ids = set(existing_enc[ENCOUNTER_MAPPING_TABLE_ENCID_COL].astype(str))
            enc_df = enc_df[~enc_df[ENCOUNTER_MAPPING_TABLE_ENCID_COL].astype(str).isin(existing_encounter_ids)].copy()
        if enc_df.empty:
            print("No new encounters to process.")
            return

        # Remove duplicates within enc_df itself (if any) by encounter_id, keeping the first occurrence
        enc_df = enc_df.drop_duplicates(subset=[ENCOUNTER_MAPPING_TABLE_ENCID_COL], keep='first').copy()

        # 6. Registration date column (ensure present in both frames)
        if ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL in enc_df.columns and ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL not in existing_enc.columns:
            existing_enc[ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL] = pd.NaT
        if ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL in existing_enc.columns and ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL not in enc_df.columns:
            enc_df[ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL] = pd.NaT

        # 7. Sort by patient and encounter/date
        sort_cols = [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL]
        if ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL in enc_df.columns:
            sort_cols.append(ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL)
        sort_cols.append(ENCOUNTER_MAPPING_TABLE_ENCID_COL)

        # Only assign nd_encounter_id to new encounters (not to all, to avoid duplicate nd_encounter_id for existing ones)
        # Prepare a DataFrame for only new encounters
        enc_df = enc_df.sort_values(by=sort_cols).copy()
        enc_df = enc_df.assign(source="new")

        # Assign encid_counter for new encounters, starting after the max for each patient
        enc_df["encid_counter"] = 0
        for pid in enc_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL].unique():
            mask = enc_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL] == pid
            n = mask.sum()
            # If patient has no previous encounters, start from 1, else from max+1
            prev_max = int(max_counter.get(pid, 0)) if pid in max_counter else 0
            start = prev_max + 1
            enc_df.loc[mask, "encid_counter"] = range(start, start + n)

        # Assign nd_encounter_id: one-to-one mapping with encounter_id, no duplicates
        enc_df[ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL] = enc_df.apply(
            lambda row: f"{int(row[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL])}{int(row['encid_counter']):04d}",
            axis=1
        )

        now = datetime.datetime.utcnow()
        enc_df[MAPPING_TABLE_CREATED_AT_COLUMN] = now
        enc_df[MAPPING_TABLE_UPDATED_AT_COLUMN] = now

        # 10. Only keep new rows, and only nd_patient_id as the patient identifier reference in output
        output_cols = [
            PATIENT_MAPPING_TABLE_ND_PATIENTID_COL,
            ENCOUNTER_MAPPING_TABLE_ENCID_COL,
            ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL,
            ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL,
            MAPPING_TABLE_CREATED_AT_COLUMN,
            MAPPING_TABLE_UPDATED_AT_COLUMN,
        ]
        only_new = enc_df[output_cols]
        if not only_new.empty:
            only_new.to_sql(
                ENCOUNTER_MAPPING_TABLE,
                con=self.mapping_engine,
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi",
            )
            nd_logger.info(
                f"Queue: {self.queue_id} - Appended {len(only_new)} new encounters to {ENCOUNTER_MAPPING_TABLE}."
            )
        else:
            nd_logger.info(f"Queue: {self.queue_id} - No new encounters to update.")
