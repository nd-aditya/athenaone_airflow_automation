from core.dbPkg.dbhandler import NDDBHandler
from deIdentification.nd_logger import nd_logger
from typing import TypedDict, Literal
from collections import defaultdict



class Condition(TypedDict):
    source_column: str
    reference_table: str
    column_name: str

class JoinCondition(TypedDict):
    source_table: str
    conditions: list[Condition]
    destination_column: str
    destination_column_type: Literal["patient_id", "encounter_id"]

class ReferenceMapping(TypedDict):
    reference_mapping: dict
    source_column: str
    type: Literal["patient_id", "encounter_id"]

def chunk_list(lst, chunk_size=2000):
    """Splits a list into smaller chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def create_reference_mapping(
    sourcedb: NDDBHandler,
    all_rows: list[dict],
    join_condition: JoinCondition
) -> dict:
    if not join_condition:
        return {}

    nd_logger.info(
        f"loading the reference mapping for the config: {join_condition}"
    )

    source_table_name = join_condition["source_table"]
    initial_source_column = join_condition["conditions"][0]["source_column"]
    destination_column = join_condition["destination_column"]

    # Initial identity mapping
    original_mapping = {
        row[initial_source_column]: row[initial_source_column]
        for row in all_rows
    }
    current_mapping = original_mapping.copy()

    for idx, condition in enumerate(join_condition["conditions"]):
        reference_table = condition["reference_table"]
        reference_column = condition["column_name"]

        next_column = (
            join_condition["conditions"][idx + 1]["source_column"]
            if idx + 1 < len(join_condition["conditions"])
            else destination_column
        )

        column_values = list(current_mapping.values())

        # Fetch reference rows in chunks
        reference_rows = []
        for chunk in chunk_list(column_values):
            result = sourcedb.get_rows_where_column_values_in(
                reference_table,
                reference_column,
                chunk
            )
            reference_rows.extend(result)
        # Build lookup: reference_column -> rows
        reference_lookup = defaultdict(list)
        for row in reference_rows:
            reference_lookup[row[reference_column]].append(row)

        # Build new mapping, ignoring NULLs and picking first non-null
        new_mapping = {}

        for original_key, current_value in current_mapping.items():
            mapped_value = None

            for row in reference_lookup.get(current_value, []):
                if row.get(next_column) is not None:
                    mapped_value = row[next_column]
                    break  # first non-null wins

            new_mapping[original_key] = mapped_value

        current_mapping = new_mapping

    nd_logger.info(
        f"loading done for reference mapping for the config: {join_condition}"
    )

    return {
        "reference_mapping": current_mapping,
        "source_column": initial_source_column,
        "type": join_condition["destination_column_type"],
    }


# def create_reference_mapping(sourcedb: NDDBHandler, all_rows: list[dict], join_condition: JoinCondition) -> dict:
#     if not join_condition:
#         return {}
#     nd_logger.info(f"loading the reference mapping for tha config: {join_condition}")
#     source_table_name = join_condition["source_table"]
#     initial_source_column = join_condition["conditions"][0]["source_column"]
#     destination_column = join_condition["destination_column"]

#     original_mapping = {row[initial_source_column]: row[initial_source_column] for row in all_rows}
#     current_mapping = original_mapping.copy()

#     for idx, condition in enumerate(join_condition["conditions"]):
#         reference_table = condition["reference_table"]
#         reference_column = condition["column_name"]
#         next_column = (join_condition["conditions"][idx + 1]["source_column"] 
#                       if idx + 1 < len(join_condition["conditions"]) 
#                       else destination_column)
        
#         column_values = list(current_mapping.values())
#         reference_rows = []
#         for chunk in chunk_list(column_values):
#             result = sourcedb.get_rows_where_column_values_in(
#                 reference_table, reference_column, chunk
#             )
#             reference_rows.extend(result)
#         # reference_rows = sourcedb.get_rows_where_column_values_in(
#         #     reference_table, reference_column, column_values
#         # )

#         new_mapping = {}
#         found_mapping_for = []
#         for original_key, current_value in current_mapping.items():
#             for row in reference_rows:
#                 if row[reference_column] == current_value:
#                     new_mapping[original_key] = row[next_column]
#                     found_mapping_for.append(original_key)
#                     break
#         for original_key, current_value in current_mapping.items():
#             if original_key not in found_mapping_for:
#                 new_mapping[original_key] = None
#         current_mapping = new_mapping
#     nd_logger.info(f"loading done for reference mapping for tha config: {join_condition}")
#     return {'reference_mapping': current_mapping, "source_column": join_condition["conditions"][0]["source_column"], "type": join_condition["destination_column_type"]}


# # def create_reference_mapping(sourcedb: NDDBHandler, all_rows: list[dict], join_condition: JoinCondition) -> dict:
# #     if not join_condition:
# #         return {}
# #     source_table_name = join_condition["source_table"]
# #     source_column = join_condition["conditions"][0]["source_column"]
# #     destination_column = join_condition["destination_column"]

# #     mapping = {row[source_column]: None for row in all_rows}

# #     for idx, condition in enumerate(join_condition["conditions"]):
# #         source_column = condition["source_column"]
# #         reference_table = condition["reference_table"]
# #         reference_column = condition["column_name"]

# #         column_values = list(mapping.keys())

# #         reference_rows = sourcedb.get_rows_where_column_values_in(
# #             reference_table, reference_column, column_values
# #         )

# #         if idx + 1 < len(join_condition["conditions"]):
# #             next_column = join_condition["conditions"][idx + 1]["source_column"]
# #         else:
# #             next_column = destination_column

# #         new_mapping = {}
# #         for row in reference_rows:
# #             if row[reference_column] in mapping:
# #                 new_mapping[row[reference_column]] = row.get(next_column, None)

# #         mapping = new_mapping

# #     return {'reference_mapping': mapping, "source_column": join_condition["conditions"][0]["source_column"], "type": join_condition["destination_column_type"]}
