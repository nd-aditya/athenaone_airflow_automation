import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, inspect

# Examples:
# MSSQL: "mssql+pyodbc://user:password@dsnname"
# MySQL: "mysql+pymysql://user:password@host/dbname"
# Postgres: "postgresql://user:password@host/dbname"
DB_CONNECTION_STRING = "your_connection_string_here"

# ✅ Replace with your table list or load from file
# For example:



# ✅ Create SQLAlchemy engine and inspector
engine = create_engine(DB_CONNECTION_STRING)
inspector = inspect(engine)

# ✅ Get list of all table names
table_names = inspector.get_table_names()


# ✅ Collect stats
stats = []

with engine.connect() as conn:
    for table_name in table_names:
        try:
            # Row count
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            row_count = result.scalar()

            # Column count
            columns = inspector.get_columns(table_name)
            column_count = len(columns)

            stats.append({
                'table_name': table_name,
                'row_count': row_count,
                'column_count': column_count,
                'status': 'present'
            })
        except SQLAlchemyError as e:
            stats.append({
                'table_name': table_name,
                'row_count': None,
                'column_count': None,
                'status': f'error: {str(e)}'
            })

# ✅ Save to CSV
df_stats = pd.DataFrame(stats)
df_stats.to_csv('table_stats_generic.csv', index=False)
 
