import numpy as np
import random
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from datetime import datetime
from typing import List, Dict, Tuple, Any
import pandas as pd
from core.dbPkg.dbhandler import NDDBHandler


class DataGenerator:
    def __init__(self, source_handler: NDDBHandler, dest_handler: NDDBHandler):
        self.source_engine = source_handler.engine
        self.dest_engine = dest_handler.engine
        current_seed = int(datetime.now().timestamp() * 1000000) % 2147483647
        np.random.seed(current_seed)
        random.seed(current_seed)

    def get_total_rows(self, table_name: str) -> int:
        with self.dest_engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()

    def calculate_sample_size(self, n: int) -> int:
        if n <= 300:
            return n
        elif n <= 1000:
            return random.randint(300, 500)
        elif n <= 5000:
            return random.randint(500, 800)
        elif n <= 10000:
            return random.randint(800, 1000)
        elif n <= 100000:
            return min(3000 + int((n - 10000) / 30000) * 1000, 5000)
        else:
            return 5000

    def get_random_sample(self, table_name: str, sample_size: int) -> pd.DataFrame:
        query = text(
            f"""
            SELECT *
            FROM {table_name}
            ORDER BY RAND()
            LIMIT :sample_size
        """
        )
        with self.dest_engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"sample_size": sample_size})
        return df

    def get_source_sample_in_batches(
        self, engine: Engine, table_name: str, nd_auto_increment_start_value: int, nd_auto_increment_end_value: int
    ) -> pd.DataFrame:
        """
        Fetch rows in batches from MSSQL or MySQL automatically handling pagination syntax.
        """
        # Detect database dialect
        dialect = engine.dialect.name.lower()

        if dialect in ("mssql", "microsoft"):
            query = text(f"""
                SELECT *
                FROM `{table_name}` where nd_auto_increment_id >= :nd_auto_increment_start_value and nd_auto_increment_id < :nd_auto_increment_end_value 
            """)
            params = {"nd_auto_increment_start_value": nd_auto_increment_start_value, "nd_auto_increment_end_value": nd_auto_increment_end_value}

        elif dialect in ("mysql", "mariadb"):
            query = text(f"""
                SELECT *
                FROM `{table_name}` where nd_auto_increment_id >= :nd_auto_increment_start_value and nd_auto_increment_id < :nd_auto_increment_end_value 
            """)
            params = {"nd_auto_increment_start_value": nd_auto_increment_start_value, "nd_auto_increment_end_value": nd_auto_increment_end_value}

        else:
            raise ValueError(f"Unsupported database dialect: {dialect}")

        # Execute query safely
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
            return df

    def get_dest_sample_in_batches(
        self, engine: Engine, table_name: str, source_df: pd.DataFrame
    ) -> pd.DataFrame:
        nd_auto_incr_ids = source_df["nd_auto_increment_id"].tolist()
        if not nd_auto_incr_ids:
            return pd.DataFrame()

        # Create placeholders for each ID
        placeholders = ", ".join([f":id{i}" for i in range(len(nd_auto_incr_ids))])
        query = text(f"SELECT * FROM {table_name} WHERE nd_auto_increment_id IN ({placeholders})")

        # Bind parameters
        params = {f"id{i}": v for i, v in enumerate(nd_auto_incr_ids)}

        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        return df

    def get_sample_for_nd_ids(
        self, table_name: str, nd_auto_incr_ids: List[int]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        id_list_str = ",".join(map(str, nd_auto_incr_ids))
        query = text(
            f"SELECT * FROM {table_name} WHERE nd_auto_increment_id IN ({id_list_str})"
        )

        with self.dest_engine.connect() as conn:
            dest_df = pd.read_sql(query, conn)
        with self.source_engine.connect() as conn:
            source_df = pd.read_sql(query, conn)
        return source_df, dest_df

    def get_stratified_sample(
        self,
        table_name: str,
        sample_size: int,
        initial_data: pd.DataFrame,
        important_columns: List[str],
    ) -> pd.DataFrame:
        samples = initial_data.copy()
        remaining_size = sample_size - len(initial_data)

        if remaining_size <= 0:
            return initial_data

        for col in important_columns:
            col_data = initial_data[col].dropna()
            if col_data.empty:
                continue

            is_numeric = pd.api.types.is_numeric_dtype(col_data)
            samples_per_col = remaining_size // (
                len(important_columns) * 4 if is_numeric else len(col_data.unique())
            )

            if is_numeric:
                quartiles = col_data.quantile([0.25, 0.5, 0.75])
                ranges = [
                    (None, quartiles[0.25]),
                    (quartiles[0.25], quartiles[0.5]),
                    (quartiles[0.5], quartiles[0.75]),
                    (quartiles[0.75], None),
                ]

                for lower, upper in ranges:
                    where_clause = []
                    params = {"limit": samples_per_col}

                    if lower is not None:
                        where_clause.append(f"{col} > :lower")
                        params["lower"] = float(lower)
                    if upper is not None:
                        where_clause.append(f"{col} <= :upper")
                        params["upper"] = float(upper)

                    query = text(
                        f"""
                        SELECT * FROM {table_name}
                        WHERE {' AND '.join(where_clause)}
                        ORDER BY RAND()
                        LIMIT :limit
                    """
                    )
                    with self.dest_engine.connect() as conn:
                        strata_df = pd.read_sql(query, conn, params=params)
                        samples = pd.concat([samples, strata_df], ignore_index=True)

            else:
                for val in col_data.unique():
                    query = text(
                        f"""
                        SELECT * FROM {table_name}
                        WHERE {col} = :val
                        ORDER BY RAND()
                        LIMIT :limit
                    """
                    )
                    with self.dest_engine.connect() as conn:
                        strata_df = pd.read_sql(
                            query, conn, params={"val": val, "limit": samples_per_col}
                        )
                        samples = pd.concat([samples, strata_df], ignore_index=True)

        samples.drop_duplicates(inplace=True)

        if len(samples) < sample_size:
            additional = self.get_random_sample(table_name, sample_size - len(samples))
            samples = pd.concat(
                [samples, additional], ignore_index=True
            ).drop_duplicates()

        return samples.head(sample_size)

    def generate_sample(
        self,
        table_name: str,
        nd_auto_increment_start_value: int,
        nd_auto_increment_end_value: int,
        important_columns: List[str],
    ) -> Tuple[int, pd.DataFrame, pd.DataFrame]:
        source_df = self.get_source_sample_in_batches(
            self.source_engine, table_name, nd_auto_increment_start_value, nd_auto_increment_end_value
        )
        dest_df = self.get_dest_sample_in_batches(
            self.dest_engine, table_name, source_df
        )
        return nd_auto_increment_start_value, source_df, dest_df
