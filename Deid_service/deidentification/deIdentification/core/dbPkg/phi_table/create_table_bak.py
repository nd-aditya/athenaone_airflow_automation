from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Float, Date, DateTime, Text, BigInteger, SmallInteger, DECIMAL, Boolean
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from deIdentification.nd_logger import nd_logger
from typing import TypedDict, Optional

class OnePHITableConfig(TypedDict):
    primary_col: str
    other_required_columns: list[str]

class PIITableConfig(TypedDict):
    primary_column_name: Optional[str] = None
    primary_column_type: str
    upsert_instead_of_append: bool
    tables: dict[str, OnePHITableConfig]

class PIITable:
    def __init__(self, src_db_url, dest_db_url, pii_tables_config: dict[str, PIITableConfig]):
        self.src_engine = create_engine(src_db_url)
        self.dest_engine = create_engine(dest_db_url)
        self.metadata = MetaData()
        self.pii_tables_config = pii_tables_config

    def get_column_type(self, col_type):
        type_mapping = {
            'INTEGER': Integer,
            'SMALLINT': SmallInteger,
            'TEXT': Text,
            'FLOAT': Float,
            'DATE': Date,
            'DATETIME': DateTime,
            'SMALLDATETIME': DateTime,
            'DATETIME2': DateTime,
            'BIGINT': BigInteger,
            'BIT': Boolean,
            'MONEY': DECIMAL(19, 4),
            'CHAR': String,
            'NCHAR': String,
        }
        if isinstance(col_type, Text):
            return Text()
        if isinstance(col_type, String):
            return VARCHAR(col_type.length or 255)
        if isinstance(col_type, DECIMAL):
            return DECIMAL(col_type.precision or 10, col_type.scale or 2)
        return type_mapping.get(str(col_type), String)

    def _prepare_table_definition(self, pii_table_name: str, pii_table_config: PIITableConfig):
        columns = []
        with self.src_engine.connect() as src_conn:
            inspector = inspect(src_conn)
            if pii_table_config["primary_column_name"]:
                columns.append(Column(pii_table_config['primary_column_name'], Integer, primary_key=True, nullable=False))
            for table, conf in pii_table_config.get("tables", {}).items():
                if table not in inspector.get_table_names():
                    raise Exception(f"Table {table} does not exist in the source database...")
                for colconf in inspector.get_columns(table):
                    if colconf['name'] in conf.get("other_required_columns", []):
                        col_name = f"{table}_{colconf['name']}"
                        col_type = self.get_column_type(colconf['type'])
                        columns.append(Column(col_name, col_type, nullable=True))
        if len(columns) <= 1:
            raise Exception("No valid columns found. Skipping table creation.")
        return Table(pii_table_name, self.metadata, *columns, extend_existing=True)

    def create_table(self, pii_table_name: str, pii_table_config: PIITableConfig):
        pii_table = self._prepare_table_definition(pii_table_name, pii_table_config)
        self.metadata.create_all(self.dest_engine)

    def _insert_or_update_data(self, pii_table_name: str, source_table: str, config: PIITableConfig):
        pii_table = Table(pii_table_name, self.metadata, autoload_with=self.dest_engine)
        source_conf = config["tables"][source_table]
        pii_col = config.get("primary_column_name")
        source_col = source_conf["primary_col"]
        required_cols = source_conf["other_required_columns"]

        columns = [source_col] + required_cols if pii_col else required_cols
        query = f"SELECT {', '.join(columns)} FROM {source_table}"

        with self.src_engine.connect() as src_conn:
            result = src_conn.execute(text(query)).fetchall()

        data = []
        for row in result:
            row_dict = {}
            if pii_col:
                row_dict[pii_col] = row[0]
            for i, col in enumerate(required_cols):
                val = row[i + 1] if pii_col else row[i]
                if isinstance(val, str) and val == "0000-00-00":
                    val = None
                row_dict[f"{source_table}_{col}"] = val
            data.append(row_dict)

        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch = batch[:1]
            with self.dest_engine.begin() as conn:
                try:
                    insert_stmt = mysql_insert(pii_table).values(batch)
                    update_stmt = insert_stmt.on_duplicate_key_update({
                        key: insert_stmt.inserted[key] for key in batch[0] if key != pii_col
                    })
                    conn.execute(update_stmt)
                    nd_logger.info(f"Inserted batch {i // batch_size + 1} for {pii_table_name}")
                except IntegrityError as e:
                    nd_logger.error(f"Insert/upsert failed for batch {i // batch_size + 1}: {e}")

    def generate_pii_tables(self):
        for table_name, config in self.pii_tables_config.items():
            self.create_table(table_name, config)
            for source_table in config["tables"]:
                self._insert_or_update_data(table_name, source_table, config)

    def update_pii_tables(self):
        for table_name, config in self.pii_tables_config.items():
            for source_table in config["tables"]:
                self._insert_or_update_data(table_name, source_table, config)
