from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

from .base import DATA_TYPE_MAPPING, AthenaOneSynceConfig


class AnthenaOneSyncer:
    def __init__(self, client_connection_string: str, dump_connection_string: str):
        self.client_connection_string = client_connection_string
        self.dump_connection_string = dump_connection_string

        self.snowflake_engine = create_engine(self.client_connection_string)
        self.mysql_engine = create_engine(self.dump_connection_string)

        parsed = urlparse(self.client_connection_string)
        _, self.database, self.schema = parsed.path.split("/")

    def insert_data_in_batches(self, table_name, column_names, data, batch_size):
        total_rows = len(data)
        if total_rows == 0:
            print(f"No data to insert into {table_name}.")
            return

        total_batches = (total_rows // batch_size) + (
            1 if total_rows % batch_size != 0 else 0
        )

        # Correctly construct the INSERT query using %s placeholders (DO NOT format with .format())
        columns = ", ".join([f"`{col}`" for col in column_names])  # escape column names
        placeholders = ", ".join(["%s"] * len(column_names))
        insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

        conn = self.mysql_engine.raw_connection()
        try:
            cursor = conn.cursor()

            for batch_number, start in enumerate(
                range(0, total_rows, batch_size), start=1
            ):
                batch_tuples = data[start : start + batch_size]

                # Ensure each row is a tuple
                if not isinstance(batch_tuples[0], tuple):
                    batch_tuples = [tuple(row) for row in batch_tuples]

                print(
                    f"Inserting batch {batch_number}/{total_batches} into {table_name}..."
                )
                cursor.executemany(insert_query, batch_tuples)

            conn.commit()
            print(f"✅ All {total_rows} rows inserted successfully into {table_name}.")

        except Exception as e:
            print(f"❌ Error inserting data into {table_name}: {e}")
            conn.rollback()
            print("Transaction rolled back.")

        finally:
            cursor.close()
            conn.close()

    def process_table(self, contextids, table_name, batch_size, start_date, end_date):
        column_names = []

        # Fetching View Definition
        desc_query = f"DESC VIEW {self.database}.{self.schema}.{table_name};"
        try:
            with self.snowflake_engine.connect() as conn:
                result = conn.execute(text(desc_query))
                columns_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            print(f"Error querying Snowflake for view definition: {e}")
            return

        # Construct CREATE TABLE statement for MySQL
        columns_sql = []
        for _, row in columns_df.iterrows():
            col_name = row["name"]
            snowflake_type = row["type"]
            mysql_type = DATA_TYPE_MAPPING.get(snowflake_type, "TEXT")
            columns_sql.append(f"`{col_name}` {mysql_type}")
            column_names.append(col_name)

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns_sql)}
        );
        """
        try:
            with self.mysql_engine.connect() as conn:
                conn.execute(text(create_table_sql))
                print(f"Table {table_name} created in MySQL.")
        except Exception as e:
            print(f"Error creating table {table_name} in MySQL: {e}")
            return

        # Construct SELECT query
        where_clauses = [f"contextid IN {contextids}"]
        # if last_updated_after:
        where_clauses.append(f"LASTUPDATED > '{start_date}'")
        where_clauses.append(f"LASTUPDATED <= '{end_date}'")
        where_clause = " AND ".join(where_clauses)

        select_query = f"SELECT * FROM {self.database}.{self.schema}.{table_name} WHERE {where_clause};"

        try:
            with self.snowflake_engine.connect() as conn:
                result = conn.execute(text(select_query))
                data = result.fetchall()
            print(f"Fetched {len(data)} rows from Snowflake.")
        except Exception as e:
            print(f"Error fetching data from Snowflake: {e}")
            return

        self.insert_data_in_batches(table_name, column_names, data, batch_size)

    def get_all_tables(self) -> list[str]:
        # SQL query to get table names
        query = f"""
            SELECT table_name
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{self.schema}';
        """

        # Execute and get list of table names
        try:
            with self.snowflake_engine.connect() as connection:
                result = connection.execute(text(query))
                table_list = [row[0] for row in result.fetchall()]
                return table_list
        except Exception as e:
            print(f"Error querying Snowflake: {e}")

    def start_sync(self, config: AthenaOneSynceConfig, start_date: str, end_date: str):
        max_threads = 10
        all_tables = self.get_all_tables()
        contextids = config["contextids"]  # (1, 23649)

        process_table_partial = partial(
            self.process_table, contextids, start_date, end_date
        )

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [
                executor.submit(process_table_partial, table_name)
                for table_name in all_tables
            ]

            for future in as_completed(futures):
                print(future.result())

        print("All Tables have been processed successfully.")
