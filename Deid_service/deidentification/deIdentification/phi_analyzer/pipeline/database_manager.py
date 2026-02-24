"""
Database Manager for PHI De-identification Pipeline
Handles database connections, table discovery, and sample data collection
"""
import time
import logging
import random
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
import pandas as pd
from urllib.parse import quote_plus

class DatabaseManager:
    """Manages database connections and operations for PHI analysis"""
    
    def __init__(self, connection_string: str):
        """
        Initialize database manager with configuration
        
        Args:
            config: Database configuration dictionary
        """
        self.connection_string = connection_string
        self.engine = None
        self.logger = logging.getLogger(__name__)
        self._setup_connection()
    
    def _setup_connection(self) -> None:
        """Setup database connection based on configuration"""
        try:
            self.engine = create_engine(self.connection_string, echo=False)
            self.logger.info(f"Database connection established: {self.connection_string}")

        except Exception as e:
            self.logger.error(f"Failed to setup database connection: {str(e)}")
            raise
        
    def check_column_type(self, table_name: str, column_name: str, column_type: str) -> bool:
        """
        Check if the column type is as expected, handling for MySQL, MSSQL, PostgreSQL, etc.
        Args:
            table_name: Name of the table
            column_name: Name of the column
            column_type: Expected type as a string (e.g., 'numeric', 'integer', 'varchar', etc.)
        Returns:
            True if the column matches the expected type, False otherwise.
        """
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)
            for col in columns:
                if col['name'] == column_name:
                    # Normalize SQLAlchemy type to a generic type string
                    col_type_str = str(col['type']).lower()
                    self.logger.info(f"DataType checking: {table_name}.{column_name}: real -> {col_type_str}, expected: {column_type}")
                    expected_type = column_type.lower()

                    # Map generic types to possible SQL types for different DBs
                    type_map = {
                        'numeric': [
                            'numeric', 'decimal', 'float', 'real', 'double', 'number', 'money', 'smallmoney',
                            'int', 'integer', 'bigint', 'smallint', 'tinyint', 'mediumint'
                        ],
                        'string': [
                            'varchar', 'nvarchar', 'char', 'nchar', 'text', 'string', 'clob'
                        ],
                        'date': [
                            'date', 'datetime', 'smalldatetime', 'datetime2', 'timestamp', 'datetimeoffset',  # MSSQL
                        ],
                        'time': [
                            'time',
                            'timetz',  # PostgreSQL
                            'time(0)', 'time(1)', 'time(2)', 'time(3)', 'time(4)', 'time(5)', 'time(6)',  # MySQL
                        ],
                        'notes': [
                            'text', 'longtext', 'blob'
                        ],
                        'boolean': [
                            'bool', 'boolean', 'bit'
                        ],
                    }

                    # If the expected type is a generic type, check if the column type matches any mapped type
                    if expected_type in type_map:
                        for t in type_map[expected_type]:
                            if t in col_type_str:
                                return True
                        return False
                    else:
                        # If a specific type is given, do a substring match
                        return expected_type in col_type_str
            return False
        except Exception as e:
            self.logger.error(f"Failed to check column type: {str(e)}")
            raise
    
    def get_all_tables(self) -> List[str]:
        """
        Get all table names from the database
        
        Returns:
            List of table names
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()

            tables = []
            
            self.logger.info(f"Found {len(tables)} tables to analyze")
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to get table names: {str(e)}")
            raise
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get column information for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of dictionaries containing column information
        """
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)
            
            column_info = []
            for col in columns:
                column_info.append({
                    'name': col['name'],
                    'type': str(col['type']),
                    'nullable': col['nullable'],
                    'default': col.get('default'),
                    'primary_key': col.get('primary_key', False)
                })
            
            self.logger.debug(f"Found {len(column_info)} columns in table {table_name}")
            return column_info
            
        except Exception as e:
            self.logger.error(f"Failed to get columns for table {table_name}: {str(e)}")
            raise

    def get_all_valid_rows(self, table_name: str, column_name: str, chunk_size: int = 100000) -> List[Any]:
        """
        Get all rows from a specific column in chunks to avoid memory issues.
        Args:
            table_name: Name of the table
            column_name: Name of the column
            chunk_size: Number of rows to fetch per chunk (default: 10000)
        Returns:
            List of all non-null values from the column
        """
        try:
            # Determine the maximum number of rows to collect (cap at 1,000,000)
            try:
                total_table_rows = self.get_table_row_count(table_name)
            except Exception:
                total_table_rows = 0

            target_limit = 1000000 if total_table_rows <= 0 else min(1000000, total_table_rows)

            offset = 0
            all_values = []
            chunk_count = 0
            total_rows = 0
            while True:
                if len(all_values) >= target_limit:
                    self.logger.info(
                        f"Reached target limit ({target_limit}) for {table_name}.{column_name}, stopping fetch.")
                    break

                start_time = time.time()
                # Adjust current chunk to not exceed target_limit
                current_limit = min(chunk_size, target_limit - len(all_values))
                if current_limit <= 0:
                    break

                query = text(
                    f"SELECT `{column_name}` FROM `{table_name}` WHERE `{column_name}` IS NOT NULL LIMIT :limit OFFSET :offset"
                )
                with self.engine.connect() as conn:
                    rows = conn.execute(query, {"limit": current_limit, "offset": offset}).fetchall()
                    num_rows = len(rows)
                    if not rows:
                        self.logger.info(f"No more rows to fetch after chunk {chunk_count}.")
                        break
                    all_values.extend(row[0] for row in rows if row[0] is not None)
                    chunk_count += 1
                    total_rows += num_rows
                    elapsed = time.time() - start_time
                    self.logger.info(
                        f"Chunk {chunk_count}: Retrieved {num_rows} rows from {table_name}.{column_name} in {elapsed:.2f} seconds (offset={offset})"
                    )
                if num_rows < current_limit:
                    self.logger.info(f"Fetched all available non-null rows in {chunk_count} chunks, total rows: {total_rows}")
                    break
                offset += current_limit
            self.logger.info(f"Completed fetching valid rows for {table_name}.{column_name}. Total collected: {len(all_values)} (target {target_limit})")
            return all_values
        except Exception as e:
            self.logger.error(f"Failed to get all rows for {table_name}.{column_name}: {str(e)}")
            raise

    def get_sample_values(self, table_name: str, column_name: str, sample_size: int = 15) -> List[Any]:
        try:
            driver = self.config['database'].get('driver', 'postgresql')
            with self.engine.connect() as conn:
                # Try to get random non-null values directly from the DB
                if driver == 'mysql':
                    # MySQL: use RAND()
                    query = text(f"""
                        SELECT {column_name}
                        FROM {table_name}
                        WHERE {column_name} IS NOT NULL
                        LIMIT {sample_size}
                    """)
                else:
                    # PostgreSQL, SQLite, etc: use RANDOM()
                    query = text(f"""
                        SELECT {column_name} FROM {table_name}
                        WHERE {column_name} IS NOT NULL
                        ORDER BY RANDOM()
                        LIMIT :limit
                    """)
                rows = conn.execute(query, {"limit": sample_size}).fetchall()
                samples = [r[0] for r in rows]
                self.logger.info(f"collecting values: {table_name}, col: {column_name}")
                return samples
        except Exception as e:
            self.logger.info(f"Failed to get samples from {table_name}.{column_name}: {str(e)}")
            return []
    
    def validate_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            return table_name in tables
        except Exception as e:
            self.logger.error(f"Failed to validate table existence {table_name}: {str(e)}")
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get total row count for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        try:
            query = text(f"SELECT COUNT(*) FROM {table_name}")
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchone()
                return result[0] if result else 0
        except Exception as e:
            self.logger.error(f"Failed to get row count for {table_name}: {str(e)}")
            return 0
    
    def close_connection(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
