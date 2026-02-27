from sqlalchemy import create_engine, Column, BigInteger, Integer, String, MetaData, Table, text
from deIdentification.nd_logger import nd_logger
from typing import TypedDict, Optional
from core.dbPkg.mapping_loader import PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, PATIENT_MAPPING_TABLE
import pandas as pd
import datetime
import traceback

class ReferenceMappingCondition(TypedDict):
    column_name: str
    source_column: str
    reference_table: str

class ConditionGroup(TypedDict):
    conditions: list[ReferenceMappingCondition]

class ReferenceMapping(TypedDict):
    conditions: Optional[list[ReferenceMappingCondition]]  # For backward compatibility
    condition_groups: Optional[list[ConditionGroup]]  # New: supports OR logic between groups
    source_table: str
    destination_column: str
    destination_column_type: str

class OnePHITableConfig(TypedDict):
    primary_column_name: Optional[str]
    primary_column_type: str
    required_columns: list[dict[str, str]]
    reference_mapping: Optional[ReferenceMapping]


class PIITableConfig(TypedDict):
    source_tables: dict[str, OnePHITableConfig]


def _convert_dict_list_to_dict(input_dicts: list[dict]):
    output_dict = {}
    for _dict in input_dicts:
        key, value = next(iter(_dict.items()))
        output_dict[key] = value
    return output_dict


class PIITable:
    def __init__(self, src_db_url: str, master_db_url: str, mapping_db_url: str, pii_tables_config: dict[str, PIITableConfig], queue_id: int):
        self.queue_id = queue_id
        self.master_db_url = master_db_url
        self.src_engine = create_engine(src_db_url)
        self.master_engine = create_engine(master_db_url)
        self.mapping_engine = create_engine(mapping_db_url)
        self.pii_tables_config = pii_tables_config

    def __del__(self):
        """Clean up database connections when the object is destroyed."""
        try:
            if hasattr(self, 'src_engine') and self.src_engine:
                self.src_engine.dispose()
            if hasattr(self, 'master_engine') and self.master_engine:
                self.master_engine.dispose()
            if hasattr(self, 'mapping_engine') and self.mapping_engine:
                self.mapping_engine.dispose()
        except Exception as e:
            nd_logger.warning(f"Error during PIITable cleanup: {e}")

    def backup_table(self, table_name: str):
        backup_table_name = f"{table_name}_nd_backup_queue{self.queue_id}"
        with self.master_engine.connect() as conn:
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
            # Truncate the main table after backup so we rebuild from source data only
            truncate_sql = text(f"TRUNCATE TABLE `{table_name}`")
            conn.execute(truncate_sql)
            conn.commit()
            nd_logger.info(f"Queue: {self.queue_id} - Truncated table {table_name} after backup.")

    
    def get_column_mapping(self, required_columns: list[dict]) -> dict[str, str]:
        return _convert_dict_list_to_dict(required_columns)
    
    def get_all_columns_for_pii_table(self, pii_table: str):
        config = self.pii_tables_config[pii_table]
        columns = []
        for table, table_conf in config['source_tables'].items():
            columns  += [list(col.values())[0]for col in table_conf['required_columns']]
        return list(set(columns))


    def fetch_patient_ndids(self, identifier_type: str, identifiers: list) -> dict:
        from sqlalchemy import text, bindparam

        if not identifiers:
            return {}

        stmt = text(f"""
            SELECT `{identifier_type}` AS orig_id,
                `{PATIENT_MAPPING_TABLE_ND_PATIENTID_COL}` AS ndid
            FROM `{PATIENT_MAPPING_TABLE}`
            WHERE `{identifier_type}` IN :identifiers
        """).bindparams(bindparam("identifiers", expanding=True))

        with self.mapping_engine.connect() as conn:
            rows = conn.execute(stmt, {"identifiers": identifiers}).all()

        return {row.orig_id: row.ndid for row in rows}
    
    def create_table(self, pii_table: str, pii_table_config: dict[str, OnePHITableConfig]):
        """
        Create a SQL table with ND_PATIENTID NOT NULL, all other columns nullable string(500).
        """
        metadata = MetaData()
        columns = [Column(PATIENT_MAPPING_TABLE_ND_PATIENTID_COL, BigInteger, nullable=False)]
        all_other_columns = self.get_all_columns_for_pii_table(pii_table)
        for col in all_other_columns:
            if col != PATIENT_MAPPING_TABLE_ND_PATIENTID_COL:
                columns.append(Column(col, String(500), nullable=True))
        table = Table(pii_table, metadata, *columns, extend_existing=True)
        metadata.create_all(self.master_engine)
        nd_logger.info(f"Queue: {self.queue_id} - Created or replaced table `{pii_table}` with columns: {[c.name for c in columns]}")

    def _build_single_condition_group_query(self, source_table: str, conditions: list[ReferenceMappingCondition], 
                                             columns_to_select: list[str], destination_column: str, 
                                             group_idx: int = 0) -> str:
        """
        Build a SQL query for a single condition group (AND logic within the group).
        """
        source_alias = "t0"
        safe_cols = ", ".join([f"`{source_alias}`.`{c}`" for c in columns_to_select])
        final_table_alias = f"t{len(conditions)}"
        select_clause = f"SELECT {safe_cols}, `{final_table_alias}`.`{destination_column}` AS patient_id"
        
        # Build the FROM clause
        from_clause = f"FROM `{source_table}` AS `{source_alias}`"
        
        # Build joins for each condition
        join_clauses = []
        for idx, condition in enumerate(conditions):
            ref_table = condition['reference_table']
            source_col = condition['source_column']
            ref_col = condition['column_name']
            
            # Determine which table/alias to join from
            if idx == 0:
                # First join: source_table -> first reference_table
                from_table_alias = source_alias
                from_table_col = source_col
            else:
                # Subsequent joins: previous reference_table -> next reference_table
                # Use the source_column from the previous condition's reference_table
                from_table_alias = f"t{idx}"
                # The source_column in the current condition refers to a column in the previous reference_table
                from_table_col = source_col
            
            to_table_alias = f"t{idx + 1}"
            join_clauses.append(
                f"JOIN `{ref_table}` AS `{to_table_alias}` "
                f"ON `{from_table_alias}`.`{from_table_col}` = `{to_table_alias}`.`{ref_col}`"
            )
        
        # Combine all parts
        query = f"{select_clause} {from_clause} {' '.join(join_clauses)}"
        return query

    def _build_reference_mapping_query(self, source_table: str, source_conf: OnePHITableConfig, columns_to_select: list[str]) -> str:
        """
        Build a SQL query with joins based on reference_mapping conditions to get the patient_id.
        Supports both 'conditions' (backward compatibility) and 'condition_groups' (OR logic between groups).
        """
        reference_mapping = source_conf.get('reference_mapping')
        if not reference_mapping:
            raise ValueError("reference_mapping is required when primary_column_name is null")
        
        destination_column = reference_mapping['destination_column']
        
        # Check if condition_groups is provided (new format)
        condition_groups = reference_mapping.get('condition_groups')
        if condition_groups:
            # Build UNION query for multiple condition groups (OR logic)
            subqueries = []
            for group_idx, group in enumerate(condition_groups):
                conditions = group['conditions']
                if not conditions:
                    continue
                subquery = self._build_single_condition_group_query(
                    source_table, conditions, columns_to_select, destination_column, group_idx
                )
                subqueries.append(subquery)
            
            if not subqueries:
                raise ValueError("At least one condition group with conditions is required")
            
            # Combine subqueries with UNION (removes duplicates) or UNION ALL (keeps duplicates)
            # Using UNION to remove duplicates as OR logic typically implies distinct results
            query = " UNION ".join([f"({sq})" for sq in subqueries])
            nd_logger.info(f"Queue: {self.queue_id} - Built reference mapping query with {len(subqueries)} condition groups (OR logic): {query}")
            return query
        
        # Backward compatibility: use 'conditions' if condition_groups is not provided
        conditions = reference_mapping.get('conditions')
        if conditions:
            query = self._build_single_condition_group_query(
                source_table, conditions, columns_to_select, destination_column
            )
            nd_logger.info(f"Queue: {self.queue_id} - Built reference mapping query: {query}")
            return query
        
        raise ValueError("Either 'conditions' or 'condition_groups' must be provided in reference_mapping")

    def insert_or_update_data(self, pii_table_name: str, source_table: str, source_conf: OnePHITableConfig, table_columns: list[str]):
        """
        Load source data as DataFrame, map/transform/clean, load NDID, then upsert into dest DB.
        Handles both direct primary_column_name and reference_mapping cases.
        """
        # -- 1: Prepare column mapping
        db_col_to_export_col = self.get_column_mapping(source_conf['required_columns'])
        columns_to_select = list(db_col_to_export_col.keys())
        
        # -- 2: Read source table as DataFrame
        # Check if we need to use reference_mapping (when primary_column_name is null)
        primary_column_name = source_conf.get('primary_column_name')
        
        if primary_column_name is None:
            # Use reference_mapping to join tables and get patient_id
            nd_logger.info(f"Queue: {self.queue_id} - Using reference_mapping for table {source_table}")
            query = self._build_reference_mapping_query(source_table, source_conf, columns_to_select)
            src_df = pd.read_sql_query(query, self.src_engine)
            nd_logger.info(f"Queue: {self.queue_id} - Fetched {len(src_df)} rows from reference mapping query")
            if len(src_df) > 0:
                nd_logger.info(f"Queue: {self.queue_id} - Sample columns: {src_df.columns.tolist()}")
                nd_logger.info(f"Queue: {self.queue_id} - Sample patient_id values: {src_df['patient_id'].head().tolist()}")
            # The patient_id column is already in the dataframe from the join
            primary_col_name = 'patient_id'
            primary_column_type = source_conf.get('reference_mapping', {}).get('destination_column_type', source_conf['primary_column_type'])
        else:
            # Direct case: table has primary_column_name
            safe_cols = ", ".join([f"`{c}`" for c in columns_to_select])
            src_df = pd.read_sql_query(f"SELECT {safe_cols} FROM `{source_table}`", self.src_engine)
            primary_col_name = primary_column_name
            primary_column_type = source_conf['primary_column_type']
        
        # -- 3: Clean: Special case for '0000-00-00' as None, etc.
        src_df.replace("0000-00-00", pd.NaT, inplace=True)
        
        # -- 4: Rename columns for export
        # Note: patient_id from reference_mapping join should not be renamed
        export_col_mapping = db_col_to_export_col.copy()
        
        # If using reference_mapping, preserve patient_id column name
        if primary_column_name is None:
            # patient_id is already in the dataframe from the join, don't rename it
            # Only rename columns that are in the export_col_mapping
            columns_to_rename = {k: v for k, v in export_col_mapping.items() if k in src_df.columns}
            src_df = src_df.rename(columns=columns_to_rename)
            # primary_col_name remains 'patient_id' as set above
        else:
            # Direct case: rename all columns including primary
            src_df = src_df.rename(columns=export_col_mapping)
            # Map the primary column name if it was renamed
            primary_col_name = export_col_mapping.get(primary_col_name, primary_col_name)
        
        # -- 6: Fetch NDIDs for all primary_col values (batch mapping)
        if primary_col_name not in src_df.columns:
            raise ValueError(f"Primary column '{primary_col_name}' not found in dataframe. Available columns: {src_df.columns.tolist()}")
        
        primary_vals = src_df[primary_col_name].dropna().unique().tolist()
        nd_logger.info(f"Queue: {self.queue_id} - Fetching NDIDs for {len(primary_vals)} unique {primary_column_type} values")
        id_map = self.fetch_patient_ndids(primary_column_type, primary_vals)
        nd_logger.info(f"Queue: {self.queue_id} - Found {len(id_map)} NDID mappings")
        
        # -- 7: Map NDIDs; drop rows with missing NDIDs if any.
        src_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL] = src_df[primary_col_name].map(id_map)
        rows_before_drop = len(src_df)
        
        columns_to_select = table_columns + [PATIENT_MAPPING_TABLE_ND_PATIENTID_COL]
        src_df = src_df.dropna(subset=[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL])
        rows_after_drop = len(src_df)
        if rows_before_drop > rows_after_drop:
            nd_logger.warning(f"Queue: {self.queue_id} - Dropped {rows_before_drop - rows_after_drop} rows with missing NDIDs")
        cols = [c for c in columns_to_select if c in src_df.columns]
        src_df = src_df[cols]
        nd_logger.info(f"Queue: {self.queue_id} - Final dataframe has {len(src_df)} rows with columns: {cols}")
        
        # -- 8: Upsert: Insert data in batches
        ndids = src_df[PATIENT_MAPPING_TABLE_ND_PATIENTID_COL].unique().tolist()
        
        with self.master_engine.begin() as conn:
            try:
                self._insert_dataframe_in_batches(src_df, pii_table_name)
            except Exception as e:
                # keeping intentionally
                breakpoint()
                nd_logger.error(f"Queue: {self.queue_id} - Upsert failed for `{pii_table_name}` with error: {e}")
                raise e

    def _insert_dataframe_in_batches(self, src_df: pd.DataFrame, pii_table_name: str, batch_size: int = 5000):
        """
        Insert a DataFrame into the target table in batches, ensuring transaction safety.
        """
        total_rows = len(src_df)
        if total_rows == 0:
            nd_logger.info(f"No rows to insert into `{pii_table_name}`")
            return

        try:
            for start in range(0, total_rows, batch_size):
                end = min(start + batch_size, total_rows)
                batch_df = src_df.iloc[start:end]

                with self.master_engine.begin() as conn:
                    batch_df.to_sql(
                        pii_table_name,
                        conn,
                        index=False,
                        if_exists="append",
                        method="multi"
                    )

                nd_logger.info(f"Inserted rows {start}–{end-1} into `{pii_table_name}`")

        except Exception as e:
            nd_logger.error(traceback.format_exc())
            nd_logger.error(
                f"Queue: {self.queue_id} - "
                f"Batch insert failed for `{pii_table_name}` with error: {e}"
            )
            raise e

    def generate_or_update_pii_table(self):
        for pii_table_name, config in self.pii_tables_config.items():
            try:
                nd_logger.info(f"Queue: {self.queue_id} - Starting PII table generation for {pii_table_name}")
                self.backup_table(pii_table_name)
                self.create_table(pii_table_name, config)
                for source_table, source_conf in config["source_tables"].items():
                    try:
                        nd_logger.info(f"Queue: {self.queue_id} - Processing source table {source_table} for PII table {pii_table_name}")
                        self.insert_or_update_data(pii_table_name, source_table, source_conf, config['table_columns'])
                    except Exception as e:
                        nd_logger.error(f"Queue: {self.queue_id} - Failed to process source table {source_table} for PII table {pii_table_name}: {e}")
                        raise e
                nd_logger.info(f"Queue: {self.queue_id} - Successfully completed PII table generation for {pii_table_name}")
            except Exception as e:
                nd_logger.info(f"{traceback.format_exc()}")
                nd_logger.error(f"Queue: {self.queue_id} - Failed to generate PII table {pii_table_name}: {e}")
                raise e
