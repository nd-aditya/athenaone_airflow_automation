import redis
from django.conf import settings
from nd_api.schemas.table_config import TableDetailsForUI
from .schemas import EncounterMappingDict
from .utils import parse_patientid_column_string
from typing import TypedDict
from core.dbPkg import NDDBHandler
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    MetaData,
    BigInteger,
    create_engine,
    or_,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, select

Base = declarative_base()

def _get_patient_ids_from_enc_ids(encounter_ids: list[int], encounter_id_mapping: dict[int, EncounterMappingDict]) -> list[int]:
    patient_ids = []
    for encid in encounter_ids:
        if (encid is None) or (encid not in encounter_id_mapping):
            continue
        patient_ids.append(encounter_id_mapping[encid]["patient_id"])
    return patient_ids


# -------------------------------
# PIITable dynamic model
# -------------------------------
PII_TABLE_NAME = "pii_data_table"
PII_ND_PATIENT_ID_COLUMN = "nd_patient_id"
pii_base_columns = {
    "ID": Column(Integer, primary_key=True),
    PII_ND_PATIENT_ID_COLUMN: Column(BigInteger),
}

PIITable = type(
    "PIITable",
    (Base,),
    {
        "__tablename__": PII_TABLE_NAME,
        "__table_args__": {"extend_existing": True},
        **pii_base_columns,
    }
)

# -------------------------------
# InsuranceTable dynamic model
# -------------------------------
insurance_base_columns = {
    "encounter_id": Column(Integer, primary_key=True),
}
insurance_dynamic_columns = dict(parse_patientid_column_string(settings.PATIENTID_COLUMNS))

InsuranceTable = type(
    "InsuranceTable",
    (Base,),
    {
        "__tablename__": "master_insurance_table",
        "__table_args__": {"extend_existing": True},
        **insurance_base_columns,
        **insurance_dynamic_columns,
    }
)
    

class PIIDb:
    def __init__(self, pii_db_config: dict):
        self.pii_db_config = pii_db_config
        self.master_connection = self._get_master_db_connection()
        self.insurance_connection = self._get_insurance_db_connection()

    
    def _get_master_db_connection(self):
        connection_string = self.pii_db_config["master_connection_str"]
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.master_session = Session()
        return NDDBHandler(connection_string)
    
    def _get_insurance_db_connection(self):
        connection_string = self.pii_db_config["insurance_connection_str"]
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.insurance_session = Session()
        return NDDBHandler(connection_string)
    
        
    def close_master_connection(self):
        self.master_session.close()
        self.engine.dispose()

    def close_insurance_connection(self):
        self.insurance_session.close()
        self.engine.dispose()


    def get_pii_data(
        self, patient_ids: list[int],
        pid_column: str
    ) -> dict[int, dict]:
        metadata = MetaData()
        pii_table = Table('pii_data_table', metadata, autoload_with=self.master_session.bind)
        column_pid = pii_table.c[pid_column]
        stmt = pii_table.select().where(column_pid.in_(patient_ids))
        results = self.master_session.execute(stmt).fetchall()

        pii_data_dict = {}
        for row in results:
            row_dict = row._asdict()
            pii_data_dict[row_dict[pid_column]] = row_dict

        return pii_data_dict


class PIITableLoader:
    def __init__(self, pii_db_config: dict):
        self.pii_db_connection: PIIDb = None
        self.pii_db_config = pii_db_config

    def _has_notes_columns(self, table_config: TableDetailsForUI):
        for col_conf in table_config["columns_details"]:
            if col_conf["de_identification_rule"] == "NOTES":
                return True
        return False
    
    def load_pii_table(self, table_config: TableDetailsForUI, ids: list, id_column: str) -> tuple[dict[int, dict], dict[int, dict]]:
        pii_data = {}
        if not self._has_notes_columns(table_config=table_config):
            return pii_data
        if self.pii_db_connection is None:
            self.pii_db_connection = PIIDb(self.pii_db_config)
        pii_data = self.get_nd_patients_dict(ids, id_column)
        self.pii_db_connection.close_master_connection()
        return pii_data
    
    def load_insurance_table(self, table_config: TableDetailsForUI, ids: list[int], id_column: str):
        pii_data = {}
        if not self._has_notes_columns(table_config=table_config):
            return pii_data
        if self.pii_db_connection is None:
            self.pii_db_connection = PIIDb(self.pii_db_config)
        insurnace_data_dict = {}
        metadata = MetaData()
        insurance_table = Table('master_insurance_table', metadata, autoload_with=self.pii_db_connection.insurance_session.bind)
        column_pid = insurance_table.c[id_column]
        stmt = insurance_table.select().where(column_pid.in_(ids))
        results = self.pii_db_connection.insurance_session.execute(stmt).fetchall()

        for row in results:
            row_dict = row._asdict()
            if int(row_dict[id_column]) not in insurnace_data_dict:
                insurnace_data_dict[int(row_dict[id_column])] = []
            insurnace_data_dict[row_dict[id_column]].append(row_dict)
        self.pii_db_connection.close_insurance_connection()
        return {'metadata': self.pii_db_config.get("insurance_metadata", {}), "rows": insurnace_data_dict}

    def get_nd_patients_dict(
        self,
        ids: list[int],
        id_column: str
    ) -> dict[int, dict]:
        if settings.DB_CACHER == "REDIS":
            redis_client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
            redis_keys = [f"pii_data_{id_column}:{patient_id}" for patient_id in ids]
            pipeline = redis_client.pipeline()
            for key in redis_keys:
                pipeline.hgetall(key)
            results = pipeline.execute()
            pii_dict = {}
            for value in results:
                decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
                pii_dict[int(decoded_data[id_column])] = decoded_data
            return pii_dict
        else:
            return self.pii_db_connection.get_pii_data(ids, id_column)
