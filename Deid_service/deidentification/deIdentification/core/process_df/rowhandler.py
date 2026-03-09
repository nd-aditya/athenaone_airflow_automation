import pandas as pd
from nd_api_v2.models import IgnoreRowsDeIdentificaiton
from deIdentification.nd_logger import nd_logger
import json


class InvalidRowHandler:
    def __init__(self, queue_id: str, table_name: str, key_phi_columns: tuple):
        self.queue_id = queue_id
        self.table_name = table_name
        self.key_phi_columns = key_phi_columns

        nd_logger.info(
            f"[InvalidRowHandler] Initialized for queue_id: '{queue_id}', table: '{table_name}'"
        )

    def handle(self, df: pd.DataFrame) -> pd.DataFrame:
        # If key_phi_columns is completely empty, skip ignoring
        if not self.key_phi_columns or all(not bool(elem) for elem in self.key_phi_columns):
            nd_logger.info("[InvalidRowHandler] key_phi_columns is empty ([], {}, []). Skipping row ignoring.")
            return df

        # Continue with normal invalid row handling
        if "_resolved_nd_patient_id" not in df.columns:
            nd_logger.warning("[InvalidRowHandler] Column '_resolved_nd_patient_id' not found. Returning original DataFrame.")
            return df

        invalid_mask = df["_resolved_nd_patient_id"].isna()
        ignored_df = df[invalid_mask].copy()

        if ignored_df.empty:
            nd_logger.info("[InvalidRowHandler] No invalid rows found. Nothing to ignore.")
            return df

        nd_logger.info(
            f"[InvalidRowHandler] Found {len(ignored_df)} rows with missing _resolved_nd_patient_id. Preparing to save..."
        )

        rows_to_save = []
        for _, row in ignored_df.iterrows():
            row_dict = row.apply(
                lambda x: "None" if pd.isna(x) else str(x).replace("\x00", "")
            ).to_dict()
            try:
                row_json = json.loads(json.dumps(row_dict, default=str))
                rows_to_save.append(
                    IgnoreRowsDeIdentificaiton(
                        queue_id=self.queue_id, table_name=self.table_name, row=row_json
                    )
                )
            except Exception as e:
                nd_logger.error(f"[InvalidRowHandler] Failed to serialize row: {e}")

        if rows_to_save:
            try:
                IgnoreRowsDeIdentificaiton.objects.bulk_create(
                    rows_to_save, batch_size=1000
                )
                nd_logger.info(
                    f"[InvalidRowHandler] Saved {len(rows_to_save)} rows to IgnoreRowsDeIdentificaiton."
                )
            except Exception as e:
                nd_logger.error(
                    f"[InvalidRowHandler] Failed to save ignored rows to database: {e}"
                )

        return df[~invalid_mask].copy()
