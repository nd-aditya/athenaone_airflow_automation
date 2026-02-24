import pandas as pd
from sqlalchemy import create_engine, MetaData, inspect

# Replace with your database connection string
conn_str = "mysql+pymysql://root:123456789@localhost:3306/dummy_mapping_schema_output"

engine = create_engine(conn_str)
inspector = inspect(engine)

# Get all table names
all_tables = inspector.get_table_names()

# Prepare list to store table info
table_info = []

for table_name in all_tables:
    pk_columns = inspector.get_pk_constraint(table_name).get("constrained_columns", [])
    # Join multiple columns with comma if composite key
    pk_str = ",".join(pk_columns)
    table_info.append({"table_name": table_name, "unique_key_column": pk_str})

# Convert to DataFrame
df = pd.DataFrame(table_info)

# Save to CSV
df.to_csv("tables_primary_keys.csv", index=False)

print("CSV file 'tables_primary_keys.csv' has been created successfully!")