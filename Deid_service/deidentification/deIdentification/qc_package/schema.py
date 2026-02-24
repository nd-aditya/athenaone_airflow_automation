from typing import TypedDict


class ColumnRemarks(TypedDict, total=False):
    length_verification_failed: int
    prefix_verification_failed: int


class ColumnQCResult(TypedDict):
    passed_count: int
    failed_count: int
    failure_reasons: list[int]
    failure_nd_auto_incr_ids: list[int]
    remarks: dict


class FinalQCResult(TypedDict):
    is_qc_passed: bool
    reason: str


class OutputSchemaForTable(TypedDict):
    source_rows_count: int
    dest_rows_count: int
    ignore_rows_count: int
    column_qc_result: dict[str, ColumnQCResult]
    final_qc_result: FinalQCResult
