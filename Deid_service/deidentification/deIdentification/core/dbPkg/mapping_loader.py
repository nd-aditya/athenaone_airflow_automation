import redis
from django.conf import settings
from .schemas import PatientMappingDict, EncounterMappingDict, MappingDbConfig

from typing import TypedDict
from core.dbPkg import NDDBHandler
from .schemas import (
    PatientMappingDict,
    EncounterMappingDict,
    MappingDbConfig,
)
from .utils import parse_patientid_column_string
from sqlalchemy import (
    DateTime,
    Table,
    Column,
    Integer,
    String,
    MetaData,
    create_engine,
    or_,
    func,
)
from sqlalchemy.orm import sessionmaker

from django.conf import settings
from sqlalchemy import (
    Column,
    Integer,
    String,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
MAPPING_TABLE_CREATED_AT_COLUMN = "created_at"
MAPPING_TABLE_UPDATED_AT_COLUMN = "updated_at"


# -------------------------------
# PatientMapping dynamic model
# -------------------------------
PATIENT_MAPPING_TABLE = "patient_mapping_table"
PATIENT_MAPPING_TABLE_ND_PATIENTID_COL = "nd_patient_id"
PATIENT_MAPPING_TABLE_OFFSET_COL = "offset"
PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL = "registration_date"
# Fixed columns
patient_base_columns = {
    PATIENT_MAPPING_TABLE_ND_PATIENTID_COL: Column(Integer, primary_key=True),
    PATIENT_MAPPING_TABLE_OFFSET_COL: Column(Integer),
    PATIENT_MAPPING_TABLE_REGISTRATION_DATE_COL: Column(DateTime, nullable=True),
}

# Parsed dynamic columns
patient_dynamic_columns = dict(parse_patientid_column_string(settings.PATIENTID_COLUMNS))

# Combine and build the model
PatientMapping = type(
    "PatientMapping",
    (Base,),
    {
        "__tablename__": PATIENT_MAPPING_TABLE,
        "__table_args__": {"extend_existing": True},
        **patient_base_columns,
        **patient_dynamic_columns,
    }
)

# -------------------------------
# EncounterMapping dynamic model
# -------------------------------
ENCOUNTER_MAPPING_TABLE = "encounter_mapping_table"
ENCOUNTER_MAPPING_TABLE_ENCID_COL = "encounter_id"
ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL = "nd_encounter_id"
ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL = "encounter_date"
# Fixed columns
encounter_base_columns = {
    ENCOUNTER_MAPPING_TABLE_ENCID_COL: Column(Integer, primary_key=True),
    ENCOUNTER_MAPPING_TABLE_ND_ENCID_COL: Column(Integer),
    ENCOUNTER_MAPPING_TABLE_ENCOUNTER_DATE_COL: Column(DateTime, nullable=True),
}

encounter_dynamic_columns = dict(parse_patientid_column_string(settings.PATIENTID_COLUMNS))

EncounterMapping = type(
    "EncounterMapping",
    (Base,),
    {
        "__tablename__": ENCOUNTER_MAPPING_TABLE,
        "__table_args__": {"extend_existing": True},
        **encounter_base_columns,
        **encounter_dynamic_columns,
    }
)

class MappingDb:
    def __init__(self, mapping_db_config: MappingDbConfig):
        self.mapping_db_config = mapping_db_config
        self.inhouse_mapping_table = mapping_db_config.get(
            "inhouse_mapping_table", False
        )
        self._get_mapping_table_connection()
        # self.connection = self._get_mapping_table_connection()

    def _get_mapping_table_connection(self):
        if self.inhouse_mapping_table:
            raise NotImplementedError("In-house mapping table is not implemented")
        else:
            connection_string = self.mapping_db_config["connection_str"]
            self.engine = create_engine(connection_string)
            Base.metadata.create_all(self.engine)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            return
            # return NDDBHandler(connection_string)

    def close_connection(self):
        self.session.close()
        self.engine.dispose()
    
    def get_nd_patient_id(self, patient_identifier_value: int, patient_identifier_type: str):
        column_attr = getattr(PatientMapping, patient_identifier_type)
        record = (
            self.session.query(PatientMapping)
            .filter(column_attr == patient_identifier_value)
            .first()
        )
        if record:
            return getattr(record, PATIENT_MAPPING_TABLE_ND_PATIENTID_COL)
        return None
    
    def get_nd_patients_dict(
        self,
        ids: list[int],
        id_column: str,
        other_fetch_keys: list[str] = []
    ) -> dict[int, dict]:
        column_attr = getattr(PatientMapping, id_column)

        query = (
            self.session.query(PatientMapping)
            .filter(column_attr.in_(ids))
            .all()
        )

        mapping_dict = {}
        for row in query:
            key = getattr(row, id_column)
            mapping_dict[key] = {
                "nd_patient_id": row.nd_patient_id,
                "offset": row.offset,
                id_column: key
            }
            for _key in other_fetch_keys:
                mapping_dict[key][_key] = getattr(row, _key)
        return mapping_dict
    
    def get_nd_encounter_dict(
        self, encounter_ids: list[int], other_fetch_keys: list[str] = []
    ) -> dict[int, EncounterMappingDict]:
        query = (
            self.session.query(EncounterMapping)
            .filter(EncounterMapping.encounter_id.in_(encounter_ids))
            .all()
        )
        mapping_dict = {}
        for row in query:
            mapping_dict[row.encounter_id] = {
                "encounter_id": row.encounter_id,
                "nd_encounter_id": row.nd_encounter_id,
            }
            for key in other_fetch_keys:
                mapping_dict[row.encounter_id][key] = getattr(row, key)
        return mapping_dict
    
    def get_reverse_patients_dict(
        self, nd_patient_ids: list[int]
    ) -> dict[int, PatientMappingDict]:
        query = (
            self.session.query(PatientMapping)
            .filter(PatientMapping.nd_patient_id.in_(nd_patient_ids))
            .all()
        )
        mapping_dict = {}
        for row in query:
            mapping_dict[row.nd_patient_id] = {
                "patient_id": row.patient_id,
                "offset": row.offset,
            }
        return mapping_dict

    def get_reverse_encounter_dict(
        self, nd_encounter_ids: list[int]
    ) -> dict[int, EncounterMappingDict]:
        query = (
            self.session.query(EncounterMapping)
            .filter(EncounterMapping.nd_encounter_id.in_(nd_encounter_ids))
            .all()
        )
        mapping_dict = {}
        for row in query:
            mapping_dict[row.nd_encounter_id] = {
                "encounter_id": row.encounter_id,
                "patient_id": row.patient_id,
            }
        return mapping_dict

class MappingTableLoader:
    def __init__(self, mapping_db_config: MappingDbConfig):
        self.mapping_db_connection: MappingDb = None
        self.mapping_db_config = mapping_db_config

    def load_mapping_table(
        self, patient_id_dict: dict[str, list], encounter_ids: list[int], table_config: dict, run_config: dict
    ) -> tuple[dict[str, dict[int, PatientMappingDict]], dict[int, EncounterMappingDict]]:
        if self.mapping_db_connection is None:
            self.mapping_db_connection = MappingDb(self.mapping_db_config)
        enc_other_key_to_load = table_config.get("enc_to_pid_column_map", run_config.get("enc_to_pid_column_map", None))
        if enc_other_key_to_load is None:
            raise ValueError(f"enc_other_key_to_load mapping is not defined, please define it from config")
        enc_other_keys = [enc_other_key_to_load] if enc_other_key_to_load else []
        encounter_mapping_dict = self.get_nd_encounter_dict(encounter_ids, enc_other_keys)
        patient_id_dict = self._fill_missing_patient_ids(patient_id_dict, encounter_ids, encounter_mapping_dict, enc_other_key_to_load)
        patient_mapping_dict = self.get_nd_patients_dict(patient_id_dict)
        # encounter_mapping_dict = self.get_nd_encounter_dict(encounter_ids)
        # patient_ids = self._fill_missing_patient_ids(patient_ids, encounter_ids, encounter_mapping_dict)
        # patient_mapping_dict = self.get_nd_patients_dict(patient_ids, "patient_id")
        self.mapping_db_connection.close_connection()
        return patient_mapping_dict, encounter_mapping_dict
    
    
    def _fill_missing_patient_ids(self, patient_id_dict: dict, encounter_ids: list[int], encounter_mapping_dict: dict[int, EncounterMappingDict], enc_to_pid_column_map: str) -> list[int]:
        missing_pids = []
        pid_dict_key = "PATIENT_ID_" + enc_to_pid_column_map
        if pid_dict_key not in patient_id_dict:
            patient_id_dict[pid_dict_key] = []
        for encid in encounter_ids:
            # if (encid is None) or (encid not in encounter_mapping_dict):
            #     continue
            missing_pids.append(encounter_mapping_dict.get(encid, {}).get(enc_to_pid_column_map, None))
        patient_id_dict[pid_dict_key] = list(set(patient_id_dict[pid_dict_key] + missing_pids))
        return patient_id_dict

    def get_nd_patients_dict(
        self, patient_ids_dict: dict[str, list]
    ) -> dict[str, dict[int, PatientMappingDict]]:
        patient_mapping_dict = {}
        redis_client = None
        other_fetch_keys = [key.replace("PATIENT_ID_", "") for key in list(patient_ids_dict.keys())]
        if settings.DB_CACHER == "REDIS":
            redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
        for pid_rule, pid_ids in patient_ids_dict.items():
            pid_col = pid_rule.replace("PATIENT_ID_", "")
            if settings.DB_CACHER == "REDIS":
                redis_keys = [f"patient_id_{pid_col}:{patient_id}" for patient_id in pid_ids]
                pipeline = redis_client.pipeline()
                for key in redis_keys:
                    pipeline.hgetall(key)
                results = pipeline.execute()
                mapping_dict = {}
                for value in results:
                    decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
                    mapping_dict[int(decoded_data[pid_col])] = {
                        "nd_patient_id": int(decoded_data['nd_patient_id']),
                        "offset": int(decoded_data['offset']),
                    }
                    for key in other_fetch_keys:
                        mapping_dict[int(decoded_data[pid_col])][key] = int(decoded_data[key])
                patient_mapping_dict[pid_col] = mapping_dict
            else:
                patient_mapping_dict[pid_col] = self.mapping_db_connection.get_nd_patients_dict(pid_ids, pid_col, other_fetch_keys)
        return patient_mapping_dict
    
    def get_nd_encounter_dict(
        self, encounter_ids: list[int], other_fetch_keys: list[str] = []
    ) -> dict[int, EncounterMappingDict]:
        if settings.DB_CACHER == "REDIS":
            redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
            redis_keys = [f"encounters:{encounter_id}" for encounter_id in encounter_ids]
            pipeline = redis_client.pipeline()
            for key in redis_keys:
                pipeline.hgetall(key)
                # pipeline.get(key)
            results = pipeline.execute()
            mapping_dict = {}
            for value in results:
                if value == {}:
                    continue
                decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
                mapping_dict[int(decoded_data['encounter_id'])] = {
                    "encounter_id": int(decoded_data['encounter_id']),
                    "nd_encounter_id": int(decoded_data['nd_encounter_id']),
                    # "patient_id": int(decoded_data['patient_id']),
                }
                for key in other_fetch_keys:
                    mapping_dict[int(decoded_data['encounter_id'])][key] = decoded_data[key]
            return mapping_dict
        else:
            return self.mapping_db_connection.get_nd_encounter_dict(encounter_ids, other_fetch_keys)
