import psycopg2
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl
import logging
import io
import json
from pathlib import Path
from typing import Dict, Any, Iterator

from ..loader import DatabaseLoader

logger = logging.getLogger(__name__)

class PostgresLoader(DatabaseLoader):
    """
    A database loader for PostgreSQL.

    This class implements the DatabaseLoader interface for loading data
    efficiently into a PostgreSQL database.
    """

    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self, conn_str: str) -> None:
        """Establishes a connection to the PostgreSQL database."""
        logger.info("Connecting to PostgreSQL database...")
        try:
            self.conn = psycopg2.connect(conn_str)
            self.cursor = self.conn.cursor()
            logger.info("Connection successful.")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def cleanup(self) -> None:
        """Closes the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("PostgreSQL connection closed.")

    def _pyarrow_to_postgres_type(self, arrow_type: pa.DataType) -> str:
        """Maps a PyArrow data type to a PostgreSQL data type string."""
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "TEXT"
        elif pa.types.is_integer(arrow_type):
            return "BIGINT" # Default to BIGINT for all integers
        elif pa.types.is_floating(arrow_type):
            return "DOUBLE PRECISION"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        elif pa.types.is_date(arrow_type):
            return "DATE"
        elif pa.types.is_struct(arrow_type) or pa.types.is_list(arrow_type):
            # For nested structures, we'll store them as JSONB
            return "JSONB"
        else:
            logger.warning(f"Unsupported PyArrow type {arrow_type}. Defaulting to TEXT.")
            return "TEXT"

    def _generate_create_table_sql(self, table_name: str, schema: pa.Schema) -> str:
        """Generates a CREATE TABLE SQL statement from a PyArrow schema."""
        columns = []
        for field in schema:
            col_name = field.name
            col_type = self._pyarrow_to_postgres_type(field.type)
            columns.append(f'"{col_name}" {col_type}')

        cols_sql = ",\n  ".join(columns)
        return f"CREATE TABLE {table_name} (\n  {cols_sql}\n);"

    def prepare_staging_table(self, table_name: str, parquet_path: Path) -> None:
        """
        Creates a staging table by inferring schema from a Parquet file.
        The table is dropped if it already exists.
        """
        logger.info(f"Preparing staging table '{table_name}'...")
        # Infer schema from the first Parquet file
        first_file = next(parquet_path.glob("*.parquet"))
        parquet_schema = pq.read_schema(first_file)

        create_sql = self._generate_create_table_sql(table_name, parquet_schema)

        logger.info(f"Dropping table '{table_name}' if it exists.")
        self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

        logger.info(f"Creating table '{table_name}' with inferred schema.")
        logger.debug(f"CREATE TABLE SQL:\n{create_sql}")
        self.cursor.execute(create_sql)
        self.conn.commit()

    def bulk_load_native(self, table_name: str, parquet_path: Path) -> int:
        """
        Loads data from a directory of Parquet files into a PostgreSQL table
        using the native COPY command for high performance.
        """
        total_rows = 0

        # We need the schema to handle data conversion properly
        first_file = next(parquet_path.glob("*.parquet"))
        parquet_schema = pq.read_schema(first_file)

        # The columns in the COPY command must match the file's columns
        columns_str = ", ".join([f'"{f.name}"' for f in parquet_schema])
        copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"

        for file in sorted(parquet_path.glob("*.parquet")):
            logger.info(f"Processing file for COPY: {file.name}")

            # Use Polars for efficient streaming and data manipulation
            lf = pl.scan_parquet(file)

            # Prepare data for COPY: serialize nested types to JSON
            select_exprs = []
            for field in parquet_schema:
                col_name = field.name
                if pa.types.is_struct(field.type) or pa.types.is_list(field.type):
                    select_exprs.append(pl.col(col_name).to_json().alias(col_name))
                else:
                    select_exprs.append(pl.col(col_name))

            lf = lf.select(select_exprs)

            # Stream data to a in-memory buffer
            buffer = io.StringIO()
            lf.sink_csv(buffer, separator='\t', null_value='\\N', include_header=False)
            buffer.seek(0)

            logger.info(f"Executing COPY for {file.name}...")
            self.cursor.copy_expert(sql=copy_sql, file=buffer)
            row_count = self.cursor.rowcount
            total_rows += row_count
            logger.info(f"Copied {row_count} rows from {file.name}.")

        self.conn.commit()
        logger.info(f"Total rows loaded into '{table_name}': {total_rows}")
        return total_rows

    # --- Abstract methods not yet implemented in this step ---

    def prepare_staging_schema(self, schema_name: str) -> None:
        logger.info(f"Ensuring schema '{schema_name}' exists.")
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        self.conn.commit()

    def execute_merge_strategy(self, staging_table: str, final_table: str, primary_keys: list[str]) -> None:
        raise NotImplementedError("Merge strategy is not yet implemented.")

    def update_metadata(self, version: str, dataset: str, success: bool, row_count: int) -> None:
        raise NotImplementedError("Metadata update is not yet implemented.")
