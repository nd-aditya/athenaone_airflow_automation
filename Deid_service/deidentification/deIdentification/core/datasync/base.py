from typing import TypedDict, Union


class AthenaOneSynceConfig(TypedDict):
    contextids: tuple[int, int]


class ECWSynceConfig(TypedDict):
    contextids: tuple[int, int]


SyncerConfig = Union[AthenaOneSynceConfig, ECWSynceConfig]


class Syncer:
    
    def start_sync(self, config: SyncerConfig, start_date, end_date):
        pass


DATA_TYPE_MAPPING = {
    "BOOLEAN": "TINYINT(1)",
    "DATE": "DATE",
    "FLOAT": "FLOAT",
    # NUMBER(p,s)
    "NUMBER(1,0)": "TINYINT",
    "NUMBER(2,0)": "TINYINT",
    "NUMBER(3,0)": "TINYINT",
    "NUMBER(4,0)": "SMALLINT",
    "NUMBER(5,0)": "SMALLINT",
    "NUMBER(6,0)": "MEDIUMINT",
    "NUMBER(7,0)": "MEDIUMINT",
    "NUMBER(8,0)": "INT",
    "NUMBER(10,0)": "INT",
    "NUMBER(11,0)": "INT",
    "NUMBER(12,0)": "BIGINT",
    "NUMBER(13,0)": "BIGINT",
    "NUMBER(14,4)": "DECIMAL(14,4)",
    "NUMBER(16,0)": "BIGINT",
    "NUMBER(18,0)": "BIGINT",
    "NUMBER(19,0)": "BIGINT",
    "NUMBER(20,2)": "DECIMAL(20,2)",
    "NUMBER(20,8)": "DECIMAL(20,8)",
    "NUMBER(21,5)": "DECIMAL(21,5)",
    "NUMBER(22,0)": "BIGINT",
    "NUMBER(22,2)": "DECIMAL(22,2)",
    "NUMBER(22,3)": "DECIMAL(22,3)",
    "NUMBER(24,6)": "DECIMAL(24,6)",
    "NUMBER(28,8)": "DECIMAL(28,8)",
    "NUMBER(30,0)": "DECIMAL(30,0)",
    "NUMBER(32,2)": "DECIMAL(32,2)",
    "NUMBER(38,0)": "DECIMAL(38,0)",
    "NUMBER(38,5)": "DECIMAL(38,5)",
    "NUMBER(38,10)": "DECIMAL(38,10)",
    "NUMBER(4,2)": "DECIMAL(4,2)",
    "NUMBER(5,2)": "DECIMAL(5,2)",
    "NUMBER(5,3)": "DECIMAL(5,3)",
    "NUMBER(6,0)": "MEDIUMINT",
    "NUMBER(8,2)": "DECIMAL(8,2)",
    "NUMBER(8,3)": "DECIMAL(8,3)",
    "NUMBER(8,4)": "DECIMAL(8,4)",
    "NUMBER(8,6)": "DECIMAL(8,6)",
    "NUMBER(10,2)": "DECIMAL(10,2)",
    "NUMBER(10,4)": "DECIMAL(10,4)",
    "NUMBER(10,6)": "DECIMAL(10,6)",
    "NUMBER(11,2)": "DECIMAL(11,2)",
    "NUMBER(11,3)": "DECIMAL(11,3)",
    "NUMBER(12,1)": "DECIMAL(12,1)",
    "NUMBER(12,2)": "DECIMAL(12,2)",
    "NUMBER(12,3)": "DECIMAL(12,3)",
    "NUMBER(12,4)": "DECIMAL(12,4)",
    "NUMBER(12,6)": "DECIMAL(12,6)",
    "NUMBER(17,5)": "DECIMAL(17,5)",
    "NUMBER(18,5)": "DECIMAL(18,5)",
    "NUMBER(18,6)": "DECIMAL(18,6)",
    # TIMESTAMP
    "TIMESTAMP_NTZ(9)": "DATETIME",  # or just DATETIME depending on precision need
    # VARCHAR(n)
    "VARCHAR(1)": "VARCHAR(1)",
    "VARCHAR(2)": "VARCHAR(2)",
    "VARCHAR(6)": "VARCHAR(6)",
    "VARCHAR(7)": "VARCHAR(7)",
    "VARCHAR(10)": "VARCHAR(10)",
    "VARCHAR(11)": "VARCHAR(11)",
    "VARCHAR(12)": "VARCHAR(12)",
    "VARCHAR(13)": "VARCHAR(13)",
    "VARCHAR(18)": "VARCHAR(18)",
    "VARCHAR(20)": "VARCHAR(20)",
    "VARCHAR(28)": "VARCHAR(28)",
    "VARCHAR(30)": "VARCHAR(30)",
    "VARCHAR(50)": "VARCHAR(50)",
    "VARCHAR(16777216)": "TEXT",  # MySQL's VARCHAR max is 65535; TEXT is safer
}
