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

    def _ensure_metadata_table_exists(self) -> None:
        """Checks for and creates the metadata table if it doesn't exist."""
        table_name = "_ot_load_metadata"
        logger.info(f"Checking for metadata table '{table_name}'...")
        self.cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);",
            (table_name,)
        )
        if self.cursor.fetchone()[0]:
            logger.info(f"Metadata table '{table_name}' already exists.")
            return

        logger.info(f"Metadata table '{table_name}' not found, creating it...")
        create_sql = f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            load_timestamp TIMESTAMPTZ DEFAULT NOW(),
            opentargets_version TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            rows_loaded BIGINT,
            status TEXT NOT NULL,
            error_message TEXT
        );
        """
        self.cursor.execute(create_sql)
        self.conn.commit()
        logger.info(f"Successfully created metadata table '{table_name}'.")

    def get_last_successful_version(self, dataset: str) -> str | None:
        """
        Retrieves the version string of the last successful load for a given dataset.

        :param dataset: The name of the dataset.
        :return: The version string or None if no successful load is found.
        """
        self._ensure_metadata_table_exists()
        self.cursor.execute(
            """
            SELECT opentargets_version
            FROM _ot_load_metadata
            WHERE dataset_name = %s AND status = 'success'
            ORDER BY load_timestamp DESC
            LIMIT 1;
            """,
            (dataset,)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def _table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        schema, table = table_name.split('.')
        self.cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);",
            (schema, table)
        )
        return self.cursor.fetchone()[0]

    def _get_table_columns(self, table_name: str) -> list[str]:
        """Retrieves a list of column names for a given table."""
        schema, table = table_name.split('.')
        self.cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position;",
            (schema, table)
        )
        return [row[0] for row in self.cursor.fetchall()]

    def _get_table_schema_from_db(self, table_name: str) -> Dict[str, str]:
        """
        Retrieves a dictionary of {column_name: data_type} for a given table.
        """
        schema, table = table_name.split('.')
        self.cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
            """,
            (schema, table)
        )
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def align_final_table_schema(self, staging_table: str, final_table: str) -> None:
        """
        Aligns the schema of the final table to match the staging table.
        It adds any columns that exist in the staging table but not in the final table.
        """
        logger.info(f"Aligning schema of final table '{final_table}' to staging table '{staging_table}'.")

        staging_schema = self._get_table_schema_from_db(staging_table)
        final_schema = self._get_table_schema_from_db(final_table)

        new_columns = set(staging_schema.keys()) - set(final_schema.keys())

        if not new_columns:
            logger.info("Schemas are already aligned. No new columns to add.")
            return

        logger.info(f"Found {len(new_columns)} new columns to add: {', '.join(new_columns)}")

        # Use psycopg2's sql composition to safely quote identifiers
        from psycopg2 import sql

        for col_name in sorted(list(new_columns)): # Sort for deterministic behaviour
            col_type = staging_schema[col_name]
            logger.info(f"Adding column '{col_name}' with type '{col_type}' to '{final_table}'.")

            # Properly quote identifiers to prevent SQL injection
            alter_sql = sql.SQL("ALTER TABLE {final_table} ADD COLUMN {col_name} {col_type};").format(
                final_table=sql.Identifier(*final_table.split('.')),
                col_name=sql.Identifier(col_name),
                col_type=sql.SQL(col_type) # Type is from information_schema, so it should be safe
            )
            self.cursor.execute(alter_sql)

        self.conn.commit()
        logger.info("Successfully aligned schema.")


    def execute_merge_strategy(self, staging_table: str, final_table: str, primary_keys: list[str]) -> None:
        """
        Merges data from the staging table to the final table using an
        'INSERT ON CONFLICT' (UPSERT) strategy.

        If the final table does not exist, it is created and the data is copied.
        """
        logger.info(f"Starting merge from '{staging_table}' to '{final_table}'.")

        # 1. Handle initial load: if final table doesn't exist, create it and copy data
        if not self._table_exists(final_table):
            logger.info(f"Final table '{final_table}' does not exist. Creating and copying data...")
            self.cursor.execute(f"CREATE TABLE {final_table} AS TABLE {staging_table};")
            # Add primary key constraint to the new final table
            pk_constraint_name = f"pk_{final_table.replace('.', '_')}"
            pk_cols_str = ", ".join([f'"{k}"' for k in primary_keys])
            self.cursor.execute(f'ALTER TABLE {final_table} ADD CONSTRAINT "{pk_constraint_name}" PRIMARY KEY ({pk_cols_str});')
            logger.info(f"Successfully created and populated '{final_table}'.")
            self.conn.commit()
            return

        logger.info(f"Final table '{final_table}' exists. Performing UPSERT.")

        # 2. Get column lists for dynamic SQL generation
        all_columns = self._get_table_columns(staging_table)
        update_columns = [col for col in all_columns if col not in primary_keys]

        if not update_columns:
            logger.warning(f"No columns to update for table '{final_table}'. All columns are part of the primary key.")
            return

        # 3. Dynamically construct the 'INSERT ... ON CONFLICT' SQL
        all_cols_str = ", ".join([f'"{c}"' for c in all_columns])
        pk_cols_str = ", ".join([f'"{k}"' for k in primary_keys])
        update_clause_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_columns])

        merge_sql = f"""
        INSERT INTO {final_table} ({all_cols_str})
        SELECT {all_cols_str} FROM {staging_table}
        ON CONFLICT ({pk_cols_str}) DO UPDATE SET
        {update_clause_str};
        """

        logger.info("Executing UPSERT operation...")
        logger.debug(f"Merge SQL:\n{merge_sql}")
        self.cursor.execute(merge_sql)
        self.conn.commit()
        logger.info(f"Successfully merged data into '{final_table}'.")

    def update_metadata(self, version: str, dataset: str, success: bool, row_count: int, error_message: str = None) -> None:
        """
        Record the outcome of a load operation in the metadata table.
        """
        self._ensure_metadata_table_exists()
        status = "success" if success else "failure"
        logger.info(
            f"Updating metadata for dataset '{dataset}', version '{version}': "
            f"status={status}, rows_loaded={row_count}"
        )

        insert_sql = """
        INSERT INTO _ot_load_metadata
        (opentargets_version, dataset_name, rows_loaded, status, error_message)
        VALUES (%s, %s, %s, %s, %s);
        """
        self.cursor.execute(insert_sql, (version, dataset, row_count, status, error_message))
        self.conn.commit()
