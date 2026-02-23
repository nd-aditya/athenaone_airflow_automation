"""
Drop and recreate the incremental schema (database) before each pipeline run.
Ensures a clean staging area for extraction.
"""
from sqlalchemy import create_engine, text

from services.config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, INCREMENTAL_SCHEMA


def reset_incremental_schema() -> dict:
    """
    DROP DATABASE IF EXISTS incremental_schema; CREATE DATABASE incremental_schema.
    Connects to MySQL without selecting a database so the drop is allowed.

    Returns a small dict for XCom.
    """
    # Connect without database so we can drop/create the incremental DB
    connection_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}"
    engine = create_engine(connection_str, pool_pre_ping=True)

    schema = INCREMENTAL_SCHEMA
    with engine.connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS `{schema}`"))
        conn.commit()
        conn.execute(text(f"CREATE DATABASE `{schema}`"))
        conn.commit()

    return {"status": "ok", "schema": schema}
