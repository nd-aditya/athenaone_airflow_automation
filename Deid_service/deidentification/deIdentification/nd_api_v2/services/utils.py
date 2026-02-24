from nd_api_v2.schemas.table import TableDetailsForUI, ColumnDetailsForUI

def get_default_table_details_for_ui(columns_names: list[str]) -> TableDetailsForUI:
    """
    "ignore_rows": {
        "operator": "or",
        "columns": [{"name": "UserType", "value": 3, "condition": "neq"}]
    }
    """
    columns_details = []
    for column_name in columns_names:
        columns_details.append(
            ColumnDetailsForUI(
                column_name=column_name,
                is_phi=False,
                mask_value=None,
                de_identification_rule=None,
                add_to_phi_table=False,
                column_name_for_phi_table=None,
                ignore_rows={},
                reference_mapping={},
            )
        )
    table_details_for_ui = TableDetailsForUI(
        columns_details=columns_details,
        ignore_rows={},
        batch_size=1000,
        patient_identifier_column=None,
        patient_identifier_type=None,
        reference_mapping={},
    )
    return table_details_for_ui
