from nd_api.schemas.table_config import TableDetailsForUI
from core.ops.jointables import ReferenceMapping


def _get_patient_id_columns(table_config: TableDetailsForUI):
    patient_rule_mapping = {}
    for col_conf in table_config["columns_details"]:
        if not col_conf['is_phi']:
            continue
        rule_name = col_conf["de_identification_rule"]
        col_name = col_conf["column_name"]
        if col_conf['is_phi'] and rule_name.startswith('PATIENT_ID'):
            if rule_name in patient_rule_mapping:
                patient_rule_mapping[rule_name].append(col_name)
            else:
                patient_rule_mapping[rule_name] = [col_name]
    return patient_rule_mapping

def _get_reference_pid_columns(table_config: TableDetailsForUI):
    return [
        col_conf["column_name"]
        for col_conf in table_config["columns_details"]
        if col_conf["de_identification_rule"] == "REFERENCE_PID"
    ]


def _get_enc_id_columns(table_config: TableDetailsForUI):
    return [
        col_conf["column_name"]
        for col_conf in table_config["columns_details"]
        if col_conf["de_identification_rule"] == "ENCOUNTER_ID"
    ]

# def _get_all_distinct_patient_ids(
#     rows_list: list[dict], table_config: TableDetailsForUI
# ):
#     patient_id_dict = {}
#     patient_id_columns = _get_patient_id_columns(table_config)
#     for row in rows_list:
#         for rule_name, columns in patient_id_columns.items():
#             for col in columns:
#                 if row[col] is not None:
#                     if rule_name in patient_id_dict:
#                         patient_id_dict[rule_name].append(row[col])
#                     else:
#                         patient_id_dict[rule_name] = [row[col]]
#     return patient_id_dict

# def _get_all_distinct_reference_pids(
#     rows_list: list[dict], table_config: TableDetailsForUI
# ):
#     reference_ids = set()
#     reference_id_columns = _get_reference_pid_columns(table_config)
#     first_col = None if len(reference_id_columns)<1 else reference_id_columns[0]
#     for row in rows_list:
#         for col in reference_id_columns:
#             if col in row and row[col] is not None:
#                 reference_ids.add(row[col])
#     return first_col, list(reference_ids)

def _get_all_distinct_enc_ids(rows_list: list[dict], table_config: TableDetailsForUI):
    encounter_ids = set()
    enc_id_columns = _get_enc_id_columns(table_config)
    for row in rows_list:
        for col in enc_id_columns:
            if col in row and row[col] is not None:
                encounter_ids.add(row[col])
    return list(encounter_ids)


def _is_patient_ids_loading_required(table_config: TableDetailsForUI):
    cols = _get_patient_id_columns(table_config)
    if table_config["reference_patient_id_column"] is not None or len(cols) > 0:
        return True
    return False


def _is_enc_ids_loading_required(table_config: TableDetailsForUI):
    cols = _get_enc_id_columns(table_config)
    if table_config["reference_enc_id_column"] is not None or len(cols) > 0:
        return True
    return False


class FindIds:
    
    @classmethod
    def get_distinct_patient_id_mappings(
        cls, rows: list[dict], table_config: TableDetailsForUI, reference_mapping: ReferenceMapping
    ) -> list[int]:
        patient_id_dict: dict[str, list] = {}
        patient_id_columns = _get_patient_id_columns(table_config)
        for row in rows:
            for rule_name, columns in patient_id_columns.items():
                for col in columns:
                    if row[col] is not None:
                        if rule_name in patient_id_dict:
                            patient_id_dict[rule_name].append(row[col])
                        else:
                            patient_id_dict[rule_name] = [row[col]]
        if reference_mapping and reference_mapping["type"].startswith('PATIENT_ID'):
            pids = [value for key, value in reference_mapping.get("reference_mapping", {}).items()]
            if reference_mapping["type"] in patient_id_dict:
                patient_id_dict[reference_mapping["type"]].extends(pids)
            else:
                patient_id_dict[reference_mapping["type"]] = pids
        return patient_id_dict

    @classmethod
    def get_distinct_encounter_ids(
        cls, rows: list[dict], table_config: TableDetailsForUI, reference_mapping: ReferenceMapping
    ) -> list[int]:
        encounter_ids = []
        if _is_enc_ids_loading_required(table_config):
            encounter_ids = _get_all_distinct_enc_ids(rows, table_config)
        elif reference_mapping and reference_mapping["type"] == "ENCOUNTER_ID":
            encounter_ids = [value for key, value in reference_mapping.get("reference_mapping", {}).items()]
        return encounter_ids
