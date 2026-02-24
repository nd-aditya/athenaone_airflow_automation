from typing import Optional, Union

from nd_api.models import Table, ClientDataDump, IgnoreRowsDeIdentificaiton
from nd_api.schemas.table_config import TableDetailsForUI, ColumnDetailsForUI
from deIdentification.nd_logger import nd_logger
from core.dbPkg import NDDBHandler, MappingTableLoader, MappingDbConfig, FindIds, PIITableLoader
from core.process.rules import RulesMapping, BaseDeIdentificationRule, ReusableBag, IgnoreRowException, PatientIDDeIdntRule
from core.process.rules.helper import remove_ignored_rows
from core.process.rules import ColumnsTypeDetector, DateOffsetDeIdntRule, Rules
from core.process.rules.unstructure_rules import NotesDeIdntRule, GenericNotesDeIdntRule
from core.ops.utils import get_pii_config, get_xml_config
from core.dbPkg.schemas import EncounterMappingDict
from core.ops.jointables import create_reference_mapping, ReferenceMapping
from core.dbPkg.unversal_table import get_universal_pii_data
from worker.serialization import serialize
from tqdm import tqdm
from datetime import datetime
import time
from core.dbPkg.schemas import PatientMappingDict


def _check_if_column_is_phi(column_config: ColumnDetailsForUI) -> bool:
    return column_config["is_phi"]


def _insert_rows_to_destination_db(
    table_name: str,
    rows: list[dict],
    destination_db: NDDBHandler,
    batch_size: int = 10000,
):
    if rows:
        for i in range(0, len(rows), batch_size):
            batch_rows = rows[i : i + batch_size]
            start_time = time.time()
            destination_db.insert_to_db(batch_rows, table_name)
            end_time = time.time()
            nd_logger.info(f"*"*80)
            nd_logger.info(f"InsertTime to db : {end_time-start_time}")

def get_column_name_from_reference_column(ref_column, table_config):
        for col_conf in table_config['columns_details']:
            if col_conf["column_name"] == ref_column:
                return col_conf["de_identification_rule"].replace("PATIENT_ID_", "")

def _ignored_if_mapping_not_present(pid_type, patient_id, patient_mapping_dict, encounter_id, encounter_mapping_dict, table_config: TableDetailsForUI):
    consider_if_mapping_not_present = table_config.get("consider_if_mapping_not_present", False)
    ndid = None
    if patient_id:
        if patient_id in patient_mapping_dict[pid_type]:
            ndid = patient_mapping_dict[pid_type][int(patient_id)]["nd_patient_id"]
        elif consider_if_mapping_not_present:
            pass
        else:
            raise IgnoreRowException("value not present in mapping table")
    nd_encounter_id = None
    if encounter_id:
        if encounter_id in encounter_mapping_dict:
            nd_encounter_id = encounter_mapping_dict[int(encounter_id)]["nd_encounter_id"]
        elif consider_if_mapping_not_present:
            pass
        else:
            raise IgnoreRowException("value not present in mapping table")
    
    return ndid, nd_encounter_id

def _get_patient_and_enc_id_for_row(row: dict, table_config: TableDetailsForUI, run_config: dict, encounter_mapping_dict: dict[int, EncounterMappingDict], reference_mapping: ReferenceMapping):
    patient_id_column = table_config["reference_patient_id_column"]
    enc_id_column = table_config["reference_enc_id_column"]
    consider_if_mapping_not_present = table_config.get("consider_if_mapping_not_present", False)
    patient_id, enc_id = None, None
    ignore_row = False
    pid_type = None
    if patient_id_column:
        try:
            patient_id = int(row[patient_id_column])
            pid_type = get_column_name_from_reference_column(patient_id_column, table_config)
        except:
            ignore_row = True
            nd_logger.info(f"Not able to convert the patinet-id value to integer, {row[patient_id_column]}, type: {type(row[patient_id_column])}")
            pass
    if enc_id_column:
        try:
            enc_id = int(row[enc_id_column])
            pid_type = table_config.get("enc_to_pid_column_map", run_config.get("enc_to_pid_column_map", None))
        except:
            ignore_row = True
            nd_logger.info(f"Not able to convert the enc-id value to integer, {row[enc_id_column]}, type: {type(row[enc_id_column])}")
            pass
    if (patient_id is None) and (enc_id is None) and ignore_row:
        raise IgnoreRowException(f"ignore rows, row: {row}")
    if (patient_id_column!=None or enc_id_column!=None) and (patient_id in [0, None]) and (enc_id in [0, None]) and (not consider_if_mapping_not_present):
        raise IgnoreRowException(f"ignore rows, row: {row}")
    if (patient_id is None) and (enc_id not in [0, None]):
        pid_type = table_config.get("enc_to_pid_column_map", run_config.get("enc_to_pid_column_map", None))
        patient_id = int(encounter_mapping_dict[enc_id][pid_type])
    if (patient_id is None) and (enc_id is None):
        if reference_mapping and reference_mapping["type"].startswith("PATIENT_ID"):
            colname = reference_mapping["source_column"]
            patient_id = reference_mapping["reference_mapping"][row[colname]]
            pid_type = reference_mapping["type"].replace("PATIENT_ID_", "")
        elif reference_mapping and reference_mapping["type"] == "ENCOUNTER_ID":
            colname = reference_mapping["source_column"]
            enc_id = reference_mapping["reference_mapping"][row[colname]]
    return (patient_id, pid_type, enc_id)

def get_ids_and_id_column(rows_list: list[dict], table_config: TableDetailsForUI, run_config: dict, patient_mapping_dict: dict[int, PatientMappingDict], reference_mapping: ReferenceMapping) -> list[int]:
    pid_column = table_config['reference_patient_id_column']
    enc_id_column = table_config['reference_enc_id_column']
    id_values, id_column = [], None
    enc_to_pid_column = table_config.get("enc_to_pid_column_map", run_config.get("enc_to_pid_column_map", None))
    if pid_column:
        id_values = [row[pid_column] for row in rows_list]
        id_column = get_column_name_from_reference_column(pid_column, table_config)
    elif enc_id_column:
        if enc_to_pid_column is None:
            raise Exception(f"enc to patient-id column mapping is not defined, please define it in db run config or table_details_for_ui config")
        # id_values = list(patient_mapping_dict[f"PATIENT_ID_{enc_to_pid_column}"].keys())
        id_values = list(patient_mapping_dict[enc_to_pid_column].keys())
        id_column = enc_to_pid_column
    elif reference_mapping and reference_mapping["type"].startswith("PATIENT_ID"):
        id_column = reference_mapping["type"].replace("PATIENT_ID_", "")
        colname = reference_mapping["source_column"]
        id_values = [reference_mapping["reference_mapping"][row[colname]] for row in rows_list]
    elif reference_mapping and reference_mapping["type"] == "ENCOUNTER_ID":
        colname = reference_mapping["source_column"]
        id_values = [reference_mapping["reference_mapping"][row[colname]] for row in rows_list]
        id_column = "encounter_id"
    return id_values, id_column

def de_identify_rows(
    table_obj: Table,
    run_config: dict,
    rows_list: list[dict],
    table_config: TableDetailsForUI,
    reference_mapping: ReferenceMapping
):
    start = time.time()
    columns_config: list[ColumnDetailsForUI] = table_config["columns_details"]
    nd_logger.info(
        f"{table_obj.table_name}, Total rows before ignoring: {len(rows_list)}"
    )
    rows_list = remove_ignored_rows(rows_list, table_config)
    nd_logger.info(
        f"{table_obj.table_name}, Total rows after ignoring: {len(rows_list)}"
    )

    phi_columns = [col for col in columns_config if _check_if_column_is_phi(col)]
    nd_logger.info(f"{table_obj.table_name}, PHI columns: {phi_columns}")

    patient_ids_dict = FindIds.get_distinct_patient_id_mappings(rows_list, table_config, reference_mapping)
    distinct_encounter_ids = FindIds.get_distinct_encounter_ids(rows_list, table_config, reference_mapping)
    
    nd_logger.info(f"Found, {len(patient_ids_dict.keys())} distinct patient-id and {len(distinct_encounter_ids)} enc-ids")

    mapping_db_config: MappingDbConfig = table_obj.dump.get_mapping_db_config()
    mapping_loader = MappingTableLoader(mapping_db_config)
    patient_mapping_dict, encounter_mapping_dict = mapping_loader.load_mapping_table(
        patient_ids_dict, distinct_encounter_ids, table_config, run_config
    )
    pii_config = get_pii_config(table=table_obj)
    xml_config = get_xml_config(table=table_obj)

    pii_db_config = table_obj.dump.get_pii_db_config()
    pii_loader = PIITableLoader(pii_db_config=pii_db_config)
    
    ids, id_column = get_ids_and_id_column(rows_list, table_config, run_config, patient_mapping_dict, reference_mapping)
    pii_data_dict = pii_loader.load_pii_table(table_config, ids, id_column)
    # pii_data_dict = pii_loader.load_pii_table(table_config, patient_ids_dict, distinct_encounter_ids, encounter_mapping_dict)
    insurance_data_dict = pii_loader.load_insurance_table(table_config, ids, id_column)

    endtime = time.time()
    nd_logger.info("#"*100)
    nd_logger.info(f"master and other piii read : {endtime-start}")
    notes_rule = NotesDeIdntRule(pii_data_dict, insurance_data_dict, table_config, pii_config, xml_config)
    rows_after_deidentification = []
    for row in rows_list:
        try:
            patient_id, pid_type, encounter_id = _get_patient_and_enc_id_for_row(row, table_config, run_config, encounter_mapping_dict, reference_mapping)
            ndid, nd_encounter_id = _ignored_if_mapping_not_present(pid_type, patient_id, patient_mapping_dict, encounter_id, encounter_mapping_dict, table_config)
            reusable_bag: ReusableBag = {
                "patient_id": int(patient_id) if patient_id else None,
                "ndid": ndid,
                "enc_id": int(encounter_id) if encounter_id else None,
                "nd_encounter_id": nd_encounter_id,
            }
            reusable_bag = DateOffsetDeIdntRule.fill_offset_value(patient_mapping_dict, encounter_mapping_dict, reusable_bag, table_config, run_config)
        except (IgnoreRowException, KeyError, ValueError) as e:
            IgnoreRowsDeIdentificaiton.objects.create(
                dump_name=table_obj.dump.dump_name, table_name=table_obj.table_name, row=serialize(row)
            )
            continue
        ignore_row = False
        for column_config in phi_columns:
            coltime = time.time()
            column_value = row[column_config["column_name"]]
            rule_str = column_config["de_identification_rule"]
            if rule_str == Rules.NOTES:
                de_idntfy_val, reusable_bag = notes_rule.de_identify_value(row, column_value, column_config, reusable_bag)
            elif rule_str == Rules.GENERIC_NOTES:
                generic_notes_rule = GenericNotesDeIdntRule(pii_data_dict, insurance_data_dict, table_config, pii_config, xml_config)
                de_idntfy_val, reusable_bag = generic_notes_rule.de_identify_value(row, column_value, column_config, reusable_bag)
            else:
                try:
                    de_identification_rule_obj = PatientIDDeIdntRule if rule_str.startswith("PATIENT_ID") else RulesMapping[rule_str]
                    de_idntfy_val, reusable_bag = de_identification_rule_obj.de_identify_value(
                        table_obj.table_name,
                        column_config,
                        row,
                        patient_mapping_dict,
                        encounter_mapping_dict,
                        reusable_bag
                    )
                    nd_logger.info(
                        f"Deidentification done, table: {table_obj.table_name}, column: {column_config['column_name']}, \n{column_value} \n-> \n{de_idntfy_val}"
                    )
                except IgnoreRowException as e:
                    IgnoreRowsDeIdentificaiton.objects.create(
                        dump_name=table_obj.dump.dump_name, table_name=table_obj.table_name, row=serialize(row)
                    )
                    ignore_row = True
                    break
            row[column_config["column_name"]] = de_idntfy_val
        # nd_logger.info(f"Row after de-identification: {row}\n")
        if not ignore_row:
            rows_after_deidentification.append(row)
    return rows_after_deidentification


def _get_columns_schema_mapping(table_config: TableDetailsForUI):
    schema_mapping = {}
    rule_to_schema_mapping = ColumnsTypeDetector.get_columns_definations(table_config)
    for col_conf in table_config["columns_details"]:
        if col_conf["is_phi"]:
            rulename = col_conf["de_identification_rule"]
            if rulename.startswith("PATIENT_ID"):
                schema_mapping[col_conf["column_name"]] = rule_to_schema_mapping[Rules.PATIENT_ID]
            else:
                schema_mapping[col_conf["column_name"]] = rule_to_schema_mapping[rulename]
    return schema_mapping

def _is_patient_refer_mapping_rule_present(table_config: TableDetailsForUI):
    for col_conf in table_config.get("columns_details", []):
        if col_conf["de_identification_rule"] == Rules.REFER_PATIENT_ID:
            return True
    return False

def start_de_identification_for_table(
    table_id: int, batch_size: int, offset: int, table_config: TableDetailsForUI
):
    table_obj = Table.objects.filter(id=table_id).first()
    db_details_obj: ClientDataDump = table_obj.dump

    nd_logger.info(f"getting connection for source db: {table_obj.table_name}")
    source_db_connection: NDDBHandler = db_details_obj.get_source_db_connection()
    read_start = time.time()
    nd_logger.info(f"Reading the rows from table: {table_obj.table_name}")
    all_rows = source_db_connection.get_rows(
        table_obj.table_name, batch_size, offset
    )
    nd_logger.info(f"start generating the reference mapping: {table_id}, {batch_size}, {offset}")
    reference_mapping: ReferenceMapping = create_reference_mapping(source_db_connection, all_rows, table_config.get("reference_mapping", {}))
    nd_logger.info(f"reference mapping generation done: {table_id}, {batch_size}, {offset}")

    end_start = time.time()
    nd_logger.info("%"*100)
    nd_logger.info(f"read time: {end_start-read_start}")
    rows_after_deidntfy = de_identify_rows(table_obj, db_details_obj.run_config, all_rows, table_config, reference_mapping)
    
    destination_db: NDDBHandler = db_details_obj.get_destination_db_connection()
    column_schema_mapping = _get_columns_schema_mapping(table_config)
    source_db_connection.create_table_in_dest_if_not_exists(
        table_obj.table_name,
        destination_db,
        column_type_mapping=column_schema_mapping,
    )
    _insert_rows_to_destination_db(
        table_obj.table_name, rows_after_deidntfy, destination_db
    )
    nd_logger.info(
        f"All rows inserted to destination db: count {len(rows_after_deidntfy)}"
    )
    source_db_connection.close()
    destination_db.close()
    return {"table_id": table_id, "batch_size": batch_size, "offset": offset}
