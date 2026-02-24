#!/usr/bin/env python3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Import configuration from Django model
try:
    from nd_api_v2.services.incrementalflow.config_loader import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA
    CONN_STR = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{INCREMENTAL_SCHEMA}"
    SCHEMA = INCREMENTAL_SCHEMA
except ImportError as e:
    # Fallback values
    print(f"Warning: Could not import config_loader: {e}")
    CONN_STR = "mysql+pymysql://nd-siddharth:ndSID%402025@172.16.2.42/dump_091025"
    SCHEMA = "dump_091025"

# Table names
OLD_TABLE = "APPOINTMENT"
NEW_TABLE = "APPOINTMENT_2"

rename_sql = f"ALTER TABLE `{SCHEMA}`.`{OLD_TABLE}` RENAME TO `{SCHEMA}`.`{NEW_TABLE}`;"

def main():
    engine = create_engine(CONN_STR, pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(text(rename_sql))
            print(f"Table renamed successfully: {OLD_TABLE} -> {NEW_TABLE}")
    except SQLAlchemyError as e:
        print(f"Failed to rename table. Error: {e}")

if __name__ == "__main__":
    main()
