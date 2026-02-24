from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import pandas as pd

# Database credentials
DB_HOST = "localhost"
DB_PORT = "1433"
DB_USER = "ndasadmin"
DB_PASSWORD = "ndADMIN2025"
DB_NAME = "centricityps"

# mssql+pymssql://sa:ndADMIN2025@localhost:1433/centricityps
# Encode the password
encoded_password = quote_plus(DB_PASSWORD)

# Create the engine
source_engine = create_engine(
    f"mssql+pymssql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
source_engine = create_engine(
    f"mssql+pymssql://sa:ndADMIN2025@localhost:1433/centricityps"
)


# SQL query to extract foreign key relationships and row counts
query = """
WITH table_info AS (
    SELECT 
        t.name AS table_name,
        c.name AS column_name,
        s.name AS schema_name,
        DB_NAME() AS database_name,
        t.object_id
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.columns c ON t.object_id = c.object_id
),
fk_info AS (
    SELECT
        fk.name AS fk_name,
        OBJECT_NAME(fk.parent_object_id) AS table_name,
        pc.name AS column_name,
        OBJECT_NAME(fk.referenced_object_id) AS refer_table_name,
        rc.name AS refer_column_name,
        fk.parent_object_id,
        fk.referenced_object_id
    FROM sys.foreign_keys fk
    JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
    JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
),
row_counts AS (
    SELECT 
        t.object_id,
        SUM(p.rows) AS row_count
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.index_id IN (0,1)
    GROUP BY t.object_id
)
SELECT 
    DB_NAME() AS database_name,
    fk.table_name,
    fk.column_name,
    rc1.row_count AS table_row_count,
    fk.refer_table_name,
    fk.refer_column_name,
    rc2.row_count AS refer_table_row_count
FROM fk_info fk
LEFT JOIN row_counts rc1 ON fk.parent_object_id = rc1.object_id
LEFT JOIN row_counts rc2 ON fk.referenced_object_id = rc2.object_id
ORDER BY fk.table_name, fk.column_name;
"""


# Execute query and load into DataFrame
with source_engine.connect() as conn:
    result_df = pd.read_sql_query(text(query), conn)

# Save to CSV
result_df.to_csv("foreign_keys_with_row_counts.csv", index=False)

print("Data saved to 'foreign_keys_with_row_counts.csv'")