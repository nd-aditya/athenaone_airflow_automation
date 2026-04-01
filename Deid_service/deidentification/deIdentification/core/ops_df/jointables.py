import pandas as pd
from pandas import DataFrame
from sqlalchemy import Table, select, MetaData, Column, String, UniqueConstraint
from core.dbPkg import NDDBHandler
from deIdentification.nd_logger import nd_logger
from nd_api_v2.models import Table as TableModel
from nd_api_v2.models.scheduler_config import SchedulerConfig

from core.ops.jointables import create_reference_mapping

class ReferenceMappingDataFrameJoiner:
    def __init__(self, sourcedb: NDDBHandler, df: DataFrame, table_id ,table_config, key_phi_columns):
        self.sourcedb = sourcedb
        self.df = df

        self.table_config = table_config
        self.engine = self._get_bridge_engine()
        self.metadata = MetaData()
        self.key_phi_columns = key_phi_columns
        self.table_obj = TableModel.objects.get(id=table_id)
    
    def _get_bridge_engine(self):
        src_config = SchedulerConfig.objects.last()
        conn_str = src_config.get_bridge_db_connection_str()
        nd_handler = NDDBHandler(conn_str)
        return nd_handler.engine


    def _load_reference_table(self, table_name: str, columns: list[str], filter_column: str, filter_values: list) -> DataFrame:
        """
        Load specific columns from a reference table, filtered by provided values.
        """
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        columns_expr = [table.c[col] for col in columns]
        stmt = select(*columns_expr).where(table.c[filter_column].in_(filter_values))
        with self.engine.connect() as conn:
            ref_df = pd.read_sql(stmt, conn).drop_duplicates(subset=columns)
        return ref_df
    
    def join_dataframe(self) -> DataFrame:
        mapping = self.table_config.get("reference_mapping", "")

        if not mapping:
            nd_logger.info("[ReferenceJoiner] No reference mapping found — returning original DataFrame.")
            return self.df, self.key_phi_columns

        destination_col = mapping["destination_column"]
        destination_column_type = mapping["destination_column_type"].upper()
        source_col = mapping["conditions"][0]["source_column"]

        bridge_table_name = f"bridge_table_{self.table_obj.metadata.table_name}"

        nd_logger.info(f"[ReferenceJoiner] Using bridge table: {bridge_table_name}")

        # Load bridge table
        bridge_table = Table(bridge_table_name, self.metadata, autoload_with=self.engine)

        # Get only relevant rows from bridge table where source_col is in df
        filter_values = self.df[source_col].dropna().unique().tolist()
        if not filter_values:
            nd_logger.warning(f"[ReferenceJoiner] No values found in source column '{source_col}'. Skipping join.")
            return self.df, self.key_phi_columns

        stmt = (
            select(bridge_table.c[source_col], bridge_table.c[destination_col])
            .where(bridge_table.c[source_col].in_(filter_values))
        )

        with self.engine.connect() as conn:
            bridge_df = pd.read_sql(stmt, conn)
        
        if bridge_df.empty:
            nd_logger.warning("[ReferenceJoiner] No matching rows found in bridge table.")
            return self.df, self.key_phi_columns

        # Merge and drop duplicate join columns
        self.df[source_col] = self.df[source_col].astype(str)
        bridge_df = bridge_df.drop_duplicates(subset=[source_col])
        bridge_df[source_col] = bridge_df[source_col].astype(str)

        merged_df = pd.merge(self.df, bridge_df, how="left", on=source_col)
        
        if destination_col not in merged_df.columns:
            raise ValueError(f"[ReferenceJoiner] Destination column '{destination_col}' not found after join.")

        # Drop rows with no bridge table match (unresolvable patient identifier)
        unmatched = merged_df[destination_col].isna() | (merged_df[destination_col] == "")
        dropped = int(unmatched.sum())
        if dropped:
            nd_logger.warning(
                f"[ReferenceJoiner] Dropping {dropped} rows from '{self.table_obj.metadata.table_name}' "
                f"with no bridge table match for '{source_col}' -> '{destination_col}'."
            )
        merged_df = merged_df[~unmatched].copy()
        merged_df[destination_col] = merged_df[destination_col].fillna("")

        if destination_column_type == "ENCOUNTER_ID":
            self.key_phi_columns[0].insert(0, destination_col)
        elif destination_column_type.startswith("PATIENT_"):
            self.key_phi_columns[1].setdefault(destination_column_type, []).append(destination_col)
        elif destination_column_type == "APPOINTMENT_ID":
            self.key_phi_columns[2].insert(0, destination_col)
        nd_logger.info("[ReferenceJoiner] Successfully joined reference data using bridge table.")
        return merged_df, self.key_phi_columns

class CreateUpdateBridgeTable:

    def __init__(self, table_id: int):
        self.batch_size = 10000
        self.table_id = table_id
        self.table_obj = TableModel.objects.get(id=table_id)
        self.nd_auto_start_id = self.table_obj.nd_auto_increment_start_value
        self.nd_auto_end_id = self.table_obj.nd_auto_increment_end_value

    def build_bridge_table(self):
        bridge_table_name = f"bridge_table_{self.table_obj.metadata.table_name}"
        reference_mapping_config = self.table_obj.metadata.table_details_for_ui.get(
            "reference_mapping", {}
        )
        if not reference_mapping_config:
            nd_logger.info(
                "[BridgeTable] No reference_mapping configuration found; skipping bridge table build."
            )
            return
        try:
            conditions = reference_mapping_config["conditions"]
            initial_source_column = conditions[0]["source_column"]
            destination_column = reference_mapping_config["destination_column"]
        except (KeyError, IndexError) as exc:
            nd_logger.error(
                f"[BridgeTable] Invalid reference_mapping configuration for table "
                f"{self.table_obj.metadata.table_name}: {reference_mapping_config}. Error: {exc}"
            )
            return

        source_table_name = reference_mapping_config.get(
            "source_table", self.table_obj.metadata.table_name
        )

        scheduler_config = SchedulerConfig.objects.last()
        if scheduler_config is None:
            raise Exception("Scheduler configuration not found")

        source_db_connection: NDDBHandler = NDDBHandler(
            scheduler_config.get_historical_connection_str()
        )
        bridge_db_connection: NDDBHandler = NDDBHandler(
            scheduler_config.get_bridge_db_connection_str()
        )

        nd_logger.info(
            f"[BridgeTable] Building bridge table '{bridge_table_name}' for "
            f"source table '{source_table_name}' "
            f"using column '{initial_source_column}' -> '{destination_column}' "
            f"for nd_auto_increment_id range "
            f"{self.nd_auto_start_id} - {self.nd_auto_end_id}"
        )

        # Ensure the bridge table exists before attempting reads/inserts
        self._ensure_bridge_table_exists(
            bridge_db_connection.engine,
            bridge_table_name,
            initial_source_column,
            destination_column,
        )

        existing_source_values = set()
        try:
            # Try to load existing mappings to avoid inserting duplicates
            bridge_table = Table(
                bridge_table_name, MetaData(), autoload_with=bridge_db_connection.engine
            )
            stmt = select(bridge_table.c[initial_source_column]).distinct()
            with bridge_db_connection.engine.connect() as conn:
                existing_df = pd.read_sql(stmt, conn)
            if not existing_df.empty:
                existing_source_values = set(existing_df[initial_source_column].tolist())
        except Exception as exc:
            # Table might not exist yet or column might be missing; we just start fresh.
            nd_logger.info(
                f"[BridgeTable] Could not load existing mappings for '{bridge_table_name}' "
                f"(may be creating for the first time). Details: {exc}"
            )
        batch_size = self.batch_size

        for batch_start in range(self.nd_auto_start_id, self.nd_auto_end_id + 1, batch_size):
            batch_end = min(batch_start + batch_size - 1, self.nd_auto_end_id)
            offset = {"gt": batch_start - 1, "lt": batch_end + 1}

            nd_logger.info(
                f"[BridgeTable] Processing source rows for '{source_table_name}' with "
                f"nd_auto_increment_id between {batch_start} and {batch_end}"
            )

            df_batch = source_db_connection.get_table_as_dataframe(
                source_table_name,
                limit=batch_size,
                offset=offset,
            )

            if df_batch.empty:
                nd_logger.info("[BridgeTable] Empty batch encountered; continuing to next batch.")
                continue
            if initial_source_column not in df_batch.columns:
                nd_logger.error(
                    f"[BridgeTable] Source column '{initial_source_column}' not found in "
                    f"source table '{source_table_name}'. Available columns: {df_batch.columns.tolist()}"
                )
                break

            # Drop rows whose source value is already present in bridge table
            if existing_source_values:
                df_batch = df_batch[
                    ~df_batch[initial_source_column].isin(existing_source_values)
                ]

            if df_batch.empty:
                nd_logger.info(
                    "[BridgeTable] After removing already-mapped source values, batch is empty."
                )
                continue

            all_rows = df_batch.to_dict(orient="records")

            # Reuse existing join logic from core.ops.jointables
            try:
                mapping_result = create_reference_mapping(
                    sourcedb=source_db_connection,
                    all_rows=all_rows,
                    join_condition=reference_mapping_config,
                )
            except Exception as exc:
                nd_logger.error(
                    f"[BridgeTable] Failed to create reference mapping for batch "
                    f"{batch_start}-{batch_end}: {exc}"
                )
                continue

            reference_mapping = mapping_result.get("reference_mapping", {})
            if not reference_mapping:
                nd_logger.info(
                    f"[BridgeTable] No mappings produced for batch {batch_start}-{batch_end}."
                )
                continue

            rows_to_insert = []
            seen_source_values = set()
            for src_value, dest_value in reference_mapping.items():
                # Skip mappings without a resolved destination value
                if dest_value is None:
                    continue
                # Skip if source value already exists in bridge table or in current batch
                if src_value in existing_source_values or f'{src_value}' in existing_source_values or src_value in seen_source_values:
                    continue
                rows_to_insert.append(
                    {
                        initial_source_column: src_value,
                        destination_column: dest_value,
                    }
                )
                seen_source_values.add(src_value) 
            if not rows_to_insert:
                nd_logger.info(
                    f"[BridgeTable] No valid (non-null) mappings to insert "
                    f"for batch {batch_start}-{batch_end}."
                )
                continue

            try:
                bridge_db_connection.insert_to_db(rows_to_insert, bridge_table_name)
                nd_logger.info(
                    f"[BridgeTable] Inserted {len(rows_to_insert)} mappings into "
                    f"'{bridge_table_name}' for batch {batch_start}-{batch_end}."
                )
            except Exception as exc:
                nd_logger.error(
                    f"[BridgeTable] Failed to insert mappings into '{bridge_table_name}' "
                    f"for batch {batch_start}-{batch_end}: {exc}"
                )
                continue

            # Update the in-memory set to avoid duplicates in subsequent batches
            for row in rows_to_insert:
                existing_source_values.add(row[initial_source_column])

        nd_logger.info(
            f"[BridgeTable] Finished building bridge table '{bridge_table_name}' "
            f"for table '{source_table_name}'."
        )

    def _ensure_bridge_table_exists(
        self, engine, table_name: str, source_col: str, dest_col: str
    ) -> None:
        """
        Create the bridge table with source/destination columns if it does not exist.
        Adds a unique constraint on source_col to prevent duplicate values.
        """
        metadata = MetaData()
        Table(
            table_name,
            metadata,
            Column(source_col, String(255)),
            Column(dest_col, String(255)),
            UniqueConstraint(source_col, name=f"{table_name}_source_unique"),
        ).create(bind=engine, checkfirst=True)
        nd_logger.info(
            f"[BridgeTable] Ensured bridge table '{table_name}' exists with columns "
            f"'{source_col}' and '{dest_col}' and unique constraint on '{source_col}'."
        )