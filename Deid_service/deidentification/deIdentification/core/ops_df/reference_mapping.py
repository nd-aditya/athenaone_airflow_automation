from sqlalchemy import Table, create_engine, MetaData, select
from typing import Any, TypedDict
from nd_api.models import Table as TableModel

# reference_mapping = {
#     "conditions": [
#         {
#             "column_name": "ReportId",
#             "source_column": "reportId",
#             "reference_table": "labdata",
#         }
#     ],
#     "source_table": "labdataex",
#     "destination_column": "EncounterId",
#     "destination_column_type": "ENCOUNTER_ID",
# }

class ReferenceMapping:
    def __init__(self, table_id: str, reference_config: dict):
        self.table_obj = TableModel.objects.get(id=table_id)
        self.client_obj = self.table_obj.dump.client
        self.reference_config = reference_config
        self.engine = create_engine(
            self.client_obj.get_reference_mapping_connection_str(),
            pool_size=50,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
        )
        self.metadata = MetaData()

        self.source_column_name = self._get_source_column_name()
        self.patient_identifier_col_name = self._get_patient_identifier_col_name()
        self.patient_identifier_col_type = self._get_patient_identifier_type()
    
    def _get_source_column_name(self):
        conditions = self.reference_config.get("conditions", [])
        if len(conditions)>0:
            return conditions['source_column']
        return None
    
    def _get_patient_identifier_col_name(self):
        col_name = self.reference_config.get("destination_column_type", None)
        if col_name:
            if col_name.startswith("PATIENT"):
                return col_name.split("PATIENT_")
            else:
                return "ENCOUNTERID"
        return None
    
    def _get_patient_identifier_type(self):
        return self._get_patient_identifier_col_name()

    def get_table_name(self):
        return f"bridge_table_client:{self.client_obj.id}_{self.table_obj.table_name}"

    def update_reference_mapping_table(self, latest_dump_connection_str: str):
        """
        - we will call this function before starting the de-identification of the table, before pushing
          table for de-identification, 
        - this function will update the reference mapping before divindg the tales into multiple task
        """

    def get_respective_patient_identifier_value(self, value: Any):
        """
        Returns
        -------
        Any | None
            The identifier if a match is found, otherwise None.
        """
        table_name = self.get_table_name()
        bridge_tbl = Table(table_name, self.metadata, autoload_with=self.engine)
        if self.source_column_name not in bridge_tbl.c:
            raise KeyError(
                f"Source column '{self.source_column_name}' not found in '{table_name}'"
            )
        if self.patient_identifier_col_name not in bridge_tbl.c:
            raise KeyError(
                f"Destination column '{self.patient_identifier_col_name}' not found in '{table_name}'"
            )

        source_col = bridge_tbl.c[self.source_column_name]
        dest_col   = bridge_tbl.c[self.patient_identifier_col_name]

        stmt = (
            select(dest_col)
            .where(source_col == value)
            .limit(1)
        )

        with self.engine.connect() as conn:
            result = conn.execute(stmt).scalar_one_or_none()

        return result