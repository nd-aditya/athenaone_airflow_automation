from typing import Any
from .constants import Conditions, OPERATORS, ConditionToOperatorMapping
from nd_api.schemas.table_config import (
    IgnoreRowsConfig,
    TableDetailsForUI,
    IgnoreRowsColumn,
    ColumnDetailsForUI,
)

def _evaluate_condition(column_value: Any, condition: str, value: Any) -> bool:
    """Helper function to evaluate a single condition."""
    evaluator = ConditionToOperatorMapping.get(condition.lower())
    if not evaluator:
        raise ValueError(f"Unsupported condition: {condition}")
    return evaluator(column_value, value)

def check_if_need_to_ignore_column(
    column_config: ColumnDetailsForUI, row: dict
) -> bool:
    """
    Check if a column should be ignored based on configured rules
    """
    ignore_rows: IgnoreRowsConfig = column_config.get("ignore_column", {})
    if not ignore_rows:
        return False

    operator = ignore_rows.get("operation", OPERATORS.AND)
    entries = ignore_rows.get("columns", [])

    # Short circuit if no entries
    if not entries:
        return False

    # For AND, return False if any condition is False
    # For OR, return True if any condition is True
    for entry in entries:
        column_value = row.get(entry.get("name"))
        result = _evaluate_condition(
            column_value, entry.get("condition"), entry.get("value")
        )

        if operator == OPERATORS.AND and not result:
            return False
        if operator == OPERATORS.OR and result:
            return True

    # If we get here, for AND all conditions were True, for OR all were False
    return operator == OPERATORS.AND


def remove_ignored_rows(
    all_rows: list[dict[str, Any]], table_config: TableDetailsForUI
) -> list[dict[str, Any]]:
    """Filter out rows that should be ignored based on configured rules."""
    ignore_rows_config = table_config.get("ignore_rows", {})
    if not ignore_rows_config:
        return all_rows

    operation = ignore_rows_config.get("operation", OPERATORS.OR).lower()
    conditions_list = ignore_rows_config.get("columns", [])
    if not conditions_list:
        return all_rows

    def should_keep_row(row: dict[str, Any]) -> bool:
        conditions = [
            _evaluate_condition(row.get(entry["name"]), entry["condition"], entry["value"])
            for entry in conditions_list
        ]
        return not (all(conditions) if operation == OPERATORS.AND else any(conditions))

    return [row for row in all_rows if should_keep_row(row)]
