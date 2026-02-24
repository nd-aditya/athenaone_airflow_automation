from typing import TypedDict
from core.ops.jointables import JoinCondition

class IgnoreRowsColumn(TypedDict):
    name: str
    value: list[str]
    condition: str


class IgnoreRowsConfig(TypedDict):
    operation: str
    columns: list[IgnoreRowsColumn]


class ColumnDetailsForUI(TypedDict, total=False):
    column_name: str
    is_phi: bool
    mask_value: str
    de_identification_rule: str
    add_to_phi_table: bool
    column_name_for_phi_table: str
    ignore_rows: IgnoreRowsConfig
    reference_mapping: JoinCondition = {}


class TableDetailsForUI(TypedDict):
    enc_to_pid_column_map: str
    columns_details: list[ColumnDetailsForUI]
    ignore_rows: IgnoreRowsConfig
    batch_size: int
    reference_patient_id_column: str
    reference_enc_id_column: str
    reference_mapping: JoinCondition
