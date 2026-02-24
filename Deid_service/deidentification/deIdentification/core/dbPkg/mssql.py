from dateutil import parser
from sqlalchemy import create_engine, Table, MetaData, Column, text, UniqueConstraint, Index, PrimaryKeyConstraint, ForeignKeyConstraint
from sqlalchemy import Integer, Float, String, Date, DateTime, Text, Numeric, BigInteger, SmallInteger, DECIMAL, VARCHAR, Boolean
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TEXT, FLOAT, DATE, DATETIME, LONGTEXT, VARBINARY, LONGBLOB
from sqlalchemy.schema import Table
import sqlalchemy
import hashlib
from deIdentification.nd_logger import nd_logger
import re
from sqlalchemy import LargeBinary


type_mapping = {
    'INTEGER': Integer,
    'SMALLINT': SmallInteger,
    'TEXT': LONGTEXT,
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
    'IMAGE': LargeBinary(length=2**32-1),
    # 'VARBINARY': VARBINARY(length=2**32-1),
    'VARBINARY': LONGBLOB,
    'VARBINARY_MAX': LONGBLOB
}

def generate_short_index_name(base_name):
    # Generate a hash of the base name (index columns) and truncate it to fit within 64 characters
    short_name = base_name[:50] + hashlib.md5(base_name.encode()).hexdigest()[:10]
    return short_name[:64]

def create_table(table_name, source_engine, dest_engine, column_type_mapping):
    inspector = sqlalchemy.inspect(source_engine)
    columns = inspector.get_columns(table_name)

    metadata = MetaData()
    new_table_columns = []
    primary_key_columns = []

    for column in columns:
        col_name = column['name']
        col_type = column['type'].__class__.__name__.upper()
        col_nullable = column['nullable']
        col_autoincrement = column.get('autoincrement', False)

        if col_name in column_type_mapping:
            col_type = column_type_mapping[col_name]['type']
            if col_type == String:
                col_nullable = True
                mysql_type = col_type(column_type_mapping[col_name].get("length", 100))
            elif col_type in (Integer, DateTime, BigInteger):
                col_nullable = column_type_mapping[col_name].get("null", False)
                mysql_type = col_type()
            else:
                mysql_type = col_type()
            
        elif col_type in ['VARCHAR', 'NVARCHAR']:
            col_nullable = True
            # Extract length while handling COLLATE clause
            # match = re.search(r'(\d+)', str(column['type']))  # Extract only the numeric length
            col_length = getattr(column['type'], "length", None)
            mysql_type = LONGTEXT
            try:
                if col_length in [-1, None] or col_length > 65535:
                    mysql_type = LONGTEXT
                elif col_length > 255:
                    mysql_type = Text
                else:
                    mysql_type = VARCHAR(col_length)
            except ValueError:
                mysql_type = LONGTEXT
            # length = col_length if col_length not in (None, -1) else -1
            # length = int(match.group(1)) if match else 255
            # mysql_type = VARCHAR(length)
        elif col_type in ['DECIMAL', 'NUMERIC', 'FLOAT']:
            col_nullable = True
            # Extract precision and scale while handling unexpected characters
            # match = re.search(r'(\d+),\s*(\d+)', strs(column['type']))
            # precision, scale = (int(match.group(1)), int(match.group(2))) if match else (10, 0)
            # precision, scale = 10, 0
            # match = re.search(r'DECIMAL\((\d+)(?:,\s*(\d+))?\)', str(column['type']), re.IGNORECASE)
            # if match:
            #     precision = int(match.group(1))
            #     scale = int(match.group(2)) if match.group(2) is not None else 0

            col_type_str = str(column['type']).upper()
            precision, scale = None, None
            # if match := re.search(r'(DECIMAL|NUMERIC)\((\d+)(?:,\s*(\d+))?\)', col_type_str):
            #     precision = int(match.group(2))
            #     scale = int(match.group(3)) if match.group(3) else 0
            # elif match := re.search(r'FLOAT\((\d+)\)', col_type_str):
            #     precision = int(match.group(1))
            #     scale = None
            # elif 'FLOAT' in col_type_str:
            #     precision, scale = 53, None  # Default for FLOAT with no precision
            # else:
            #     precision, scale = 10, 0  # Fallback
            # mysql_type = DECIMAL(precision, scale)


            if 'DECIMAL' in col_type_str or 'NUMERIC' in col_type_str:
                match = re.search(r'(?:DECIMAL|NUMERIC)\((\d+)(?:,\s*(\d+))?\)', col_type_str, re.IGNORECASE)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2)) if match.group(2) else 8
                else:
                    precision, scale = 18, 10
                mysql_type = DECIMAL(precision, scale)

            elif 'FLOAT' in col_type_str:
                match = re.search(r'FLOAT\((\d+)\)', col_type_str, re.IGNORECASE)
                precision = int(match.group(1)) if match else 53
                mysql_type = FLOAT(precision)
            else:
                # Only fallback if absolutely necessary
                precision, scale = 10, 0
                mysql_type = DECIMAL(precision, scale)
        elif col_type in ['CHAR', 'NCHAR']:
            match = re.search(r'(\d+)', str(column['type']))
            length = int(match.group(1)) if match else 50
            mysql_type = String(length)
        else:
            mysql_type = type_mapping.get(col_type, String(255))  # Default fallback

        if col_autoincrement and not col_nullable and isinstance(mysql_type, (INTEGER, BigInteger)):
            primary_key_columns.append(col_name)
            new_table_columns.append(Column(col_name, mysql_type, nullable=False, autoincrement=True, primary_key=True))
        else:
            new_table_columns.append(Column(col_name, mysql_type, nullable=col_nullable))

    # Define the table
    new_table = Table(table_name, metadata, *new_table_columns)

    if primary_key_columns:
        new_table.append_constraint(PrimaryKeyConstraint(*primary_key_columns))

    # Create the table
    metadata.create_all(dest_engine)
    nd_logger.info(f"Table '{table_name}' has been created successfully.")

# def create_table(table_name, source_engine, dest_engine, column_type_mapping):
    # inspector = sqlalchemy.inspect(source_engine)
    # columns = inspector.get_columns(table_name)

    # metadata = MetaData()
    # new_table_columns = []
    # primary_key_columns = []

    # for column in columns:
    #     col_name = column['name']
    #     col_type = column['type'].__class__.__name__.upper()
    #     col_nullable = column['nullable']
    #     col_autoincrement = column.get('autoincrement', False)
        
    #     if col_name in column_type_mapping:
    #         col_type = column_type_mapping[col_name]['type']
    #         if col_type == String:
    #             mysql_type = col_type(column_type_mapping[col_name].get("length", 100))
    #         else:
    #             mysql_type = col_type()
    #     elif col_type in ['VARCHAR', 'NVARCHAR']:
    #         length = int(str(column['type']).split(')')[0].split('(')[-1])
    #         mysql_type = VARCHAR(length) if length else VARCHAR(255)
    #     elif col_type in ['DECIMAL', 'NUMERIC']:
    #         ps = str(column['type']).split('(')[-1].split(',')
    #         precision = int(ps[0])
    #         scale = int(ps[1].split(')')[0].strip())
    #         mysql_type = DECIMAL(precision if precision else 10, scale if scale else 2)
    #     elif col_type in ['CHAR', 'NCHAR']:
    #         length = int(str(column['type']).split(')')[0].split('(')[-1])
    #         mysql_type = String(length) if length else String(50)
    #     else:
    #         mysql_type = type_mapping.get(col_type)
        
    #     if col_autoincrement and not col_nullable and isinstance(mysql_type, (INTEGER, BigInteger)):
    #         primary_key_columns.append(col_name)
    #         new_table_columns.append(Column(col_name, mysql_type, nullable=False, autoincrement=True, primary_key=True))
    #     else:
    #         new_table_columns.append(Column(col_name, mysql_type, nullable=col_nullable))
    #     # if col_autoincrement and not col_nullable:
    #     #     primary_key_columns.append(col_name)

    #     # new_table_columns.append(Column(col_name, mysql_type, nullable=col_nullable))

    # # Define the table
    # new_table = Table(table_name, metadata, *new_table_columns)

    # if primary_key_columns:
    #     new_table.append_constraint(PrimaryKeyConstraint(*primary_key_columns))

    # # Create the table
    # metadata.create_all(dest_engine)
    # nd_logger.info(f"Table '{table_name}' has been created successfully.")