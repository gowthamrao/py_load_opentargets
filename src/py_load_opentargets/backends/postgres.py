import psycopg
from psycopg import sql
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl
import logging
import io
import json
from pathlib import Path
from typing import Dict, Any, Iterator, List

from ..loader import DatabaseLoader

logger = logging.getLogger(__name__)


class _ParquetStreamer:
    """
    A file-like object that streams multiple Parquet files as a single TSV stream.

    This class iterates through a list of Parquet files, processes them with
    Polars, and streams them as a single, continuous, tab-delimited text stream
    suitable for PostgreSQL's COPY command. This avoids materializing the
    entire dataset in memory or on disk as a single CSV file.
    """
    def __init__(self, files: List[Path], schema: pa.Schema):
        self._files = iter(files)
        self._schema = schema
        self._buffer = io.StringIO()
        self._eof = False

    def _load_next_file_into_buffer(self):
        """Loads the next parquet file into the in-memory buffer."""
        try:
            next_file = next(self._files)
            logger.info(f"Streaming file for COPY: {next_file.name}")

            lf = pl.scan_parquet(next_file)

            # Prepare data for COPY: serialize nested types to JSON
            select_exprs = []
            for field in self._schema:
                col_name = field.name
                if pa.types.is_struct(field.type) or pa.types.is_list(field.type):
                    select_exprs.append(pl.col(col_name).to_json().alias(col_name))
                else:
                    select_exprs.append(pl.col(col_name))
            lf = lf.select(select_exprs)

            # Reset buffer and write new data into it
            self._buffer.seek(0)
            self._buffer.truncate(0)
            lf.sink_csv(self._buffer, separator='\t', null_value='\\N', include_header=False)
            self._buffer.seek(0)

        except StopIteration:
            logger.info("All files have been streamed.")
            self._eof = True

    def read(self, size: int = -1) -> str:
        """
        Read 'size' bytes from the stream.

        If the buffer is exhausted, it attempts to load the next file.
        """
        if self._eof:
            return ""

        data = self._buffer.read(size)
        # If we didn't get any data and we are not at EOF, it means the current
        # file's buffer is empty. Time to load the next one.
        while not data and not self._eof:
            self._load_next_file_into_buffer()
            if self._eof:
                return "" # No more files to load
            data = self._buffer.read(size)

        return data


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
            self.conn = psycopg.connect(conn_str)
            self.cursor = self.conn.cursor()
            logger.info("Connection successful.")
        except psycopg.OperationalError as e:
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
        using a single, streamed, native COPY command for high performance.
        """
        logger.info(f"Starting single-stream bulk load for '{table_name}'.")

        # Get a sorted list of all parquet files in the directory
        all_files = sorted(parquet_path.glob("*.parquet"))
        if not all_files:
            logger.warning(f"No .parquet files found in '{parquet_path}'. Skipping bulk load.")
            return 0

        # We need the schema to handle data conversion properly, infer from first file
        parquet_schema = pq.read_schema(all_files[0])

        # The columns in the COPY command must match the file's columns
        columns_str = ", ".join([f'"{f.name}"' for f in parquet_schema])
        copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"

        # Create our custom streamer instance
        streamer = _ParquetStreamer(files=all_files, schema=parquet_schema)

        logger.info(f"Executing single COPY command for {len(all_files)} files...")
        with self.cursor.copy(copy_sql) as copy:
            while True:
                chunk = streamer.read(8192) # Read in 8KB chunks
                if not chunk:
                    break
                copy.write(chunk)

        total_rows = self.cursor.rowcount
        self.conn.commit()

        logger.info(f"Total rows loaded into '{table_name}' in a single transaction: {total_rows}")
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

    def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        try:
            schema, table = table_name.split('.')
        except ValueError:
            logger.error(f"Invalid table name format '{table_name}'. Expected 'schema.table'.")
            raise

        self.cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);",
            (schema, table)
        )
        return self.cursor.fetchone()[0]

    def get_table_indexes(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieves definitions for all non-primary key indexes on a table.

        :param table_name: The fully qualified name of the table.
        :return: A list of dicts, where each dict has 'name' and 'ddl' for an index.
        """
        schema, table = table_name.split('.')
        logger.info(f"Retrieving index definitions for table '{table_name}'.")
        # psycopg3 requires table names passed to pg_constraint.conrelid to be fully qualified
        # if they are not in the search path.
        fully_qualified_table = f'"{schema}"."{table}"'
        sql_query = """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        AND indexname NOT IN (
            SELECT conname
            FROM pg_constraint
            WHERE contype = 'p' AND conrelid = %s::regclass
        );
        """
        self.cursor.execute(sql_query, (schema, table, fully_qualified_table))
        indexes = [{"name": row[0], "ddl": row[1]} for row in self.cursor.fetchall()]
        if indexes:
            logger.info(f"Found {len(indexes)} non-PK indexes: {[i['name'] for i in indexes]}")
        else:
            logger.info("No non-PK indexes found.")
        return indexes

    def drop_indexes(self, indexes: List[Dict[str, str]]) -> None:
        """
        Drops a list of indexes.

        :param indexes: A list of index definition dicts from get_table_indexes.
        """
        if not indexes:
            return
        logger.info(f"Dropping {len(indexes)} indexes to improve merge performance...")
        for index in indexes:
            index_name = index['name']
            logger.info(f"Dropping index: {index_name}")
            # Use sql.Identifier for safe quoting
            self.cursor.execute(sql.SQL("DROP INDEX IF EXISTS {};").format(sql.Identifier(index_name)))
        self.conn.commit()

    def recreate_indexes(self, indexes: List[Dict[str, str]]) -> None:
        """
        Recreates a list of indexes from their DDL definitions.

        :param indexes: A list of index definition dicts from get_table_indexes.
        """
        if not indexes:
            return
        logger.info(f"Recreating {len(indexes)} indexes...")
        for index in indexes:
            logger.info(f"Recreating index using DDL: {index['ddl']}")
            # The DDL is from pg_indexes, so it's considered safe.
            self.cursor.execute(index['ddl'])
        self.conn.commit()
        logger.info("All indexes recreated successfully.")

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
        Merges data from the staging table to the final table using a
        DELETE-then-UPSERT strategy to handle additions, updates, and deletions.

        If the final table does not exist, it is created by copying the staging table.
        """
        logger.info(f"Starting merge from '{staging_table}' to '{final_table}'.")

        final_table_ident = sql.Identifier(*final_table.split('.'))
        staging_table_ident = sql.Identifier(*staging_table.split('.'))
        pk_idents = [sql.Identifier(k) for k in primary_keys]

        # 1. Handle initial load: if final table doesn't exist, create it and copy data
        if not self.table_exists(final_table):
            logger.info(f"Final table '{final_table}' does not exist. Creating and copying data...")
            self.cursor.execute(sql.SQL("CREATE TABLE {final} AS TABLE {staging};").format(
                final=final_table_ident, staging=staging_table_ident
            ))
            pk_constraint_name = sql.Identifier(f"pk_{final_table.replace('.', '_')}")
            self.cursor.execute(sql.SQL("ALTER TABLE {final} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols});").format(
                final=final_table_ident,
                pk_name=pk_constraint_name,
                pk_cols=sql.SQL(', ').join(pk_idents)
            ))
            logger.info(f"Successfully created and populated '{final_table}'.")
            self.conn.commit()
            return

        logger.info(f"Final table '{final_table}' exists. Performing DELETE-then-UPSERT.")

        # --- Stage 1: Delete records that are in the final table but not in the new staging data ---
        # This uses a `NOT EXISTS` subquery which is highly efficient in PostgreSQL.
        # It deletes any row from the final table if its primary key does not exist
        # in the staging table.
        logger.info("Identifying and deleting stale records from final table...")

        join_condition = sql.SQL(' AND ').join(
            sql.SQL("f.{pk} = s.{pk}").format(pk=pk) for pk in pk_idents
        )

        delete_sql = sql.SQL("""
            DELETE FROM {final_table} AS f
            WHERE NOT EXISTS (
                SELECT 1 FROM {staging_table} AS s
                WHERE {join_condition}
            )
        """).format(
            final_table=final_table_ident,
            staging_table=staging_table_ident,
            join_condition=join_condition
        )

        self.cursor.execute(delete_sql)
        deleted_rows = self.cursor.rowcount
        logger.info(f"Deleted {deleted_rows} stale records from '{final_table}'.")


        # --- Stage 2: Upsert records from staging into final ---
        all_columns = self._get_table_columns(staging_table)
        update_columns = [col for col in all_columns if col not in primary_keys]

        if not update_columns:
            logger.warning(f"No columns to update for table '{final_table}'. All columns are part of the primary key. Skipping UPSERT.")
            self.conn.commit()
            return

        all_cols_idents = [sql.Identifier(c) for c in all_columns]
        update_cols_idents = [sql.Identifier(c) for c in update_columns]
        update_clause = sql.SQL(', ').join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=col) for col in update_cols_idents
        )

        upsert_sql = sql.SQL("""
        INSERT INTO {final_table} ({all_cols})
        SELECT {all_cols} FROM {staging_table}
        ON CONFLICT ({pk_cols}) DO UPDATE SET
        {update_clause};
        """).format(
            final_table=final_table_ident,
            all_cols=sql.SQL(', ').join(all_cols_idents),
            staging_table=staging_table_ident,
            pk_cols=sql.SQL(', ').join(pk_idents),
            update_clause=update_clause
        )

        logger.info("Executing UPSERT operation...")
        self.cursor.execute(upsert_sql)
        upserted_rows = self.cursor.rowcount
        logger.info(f"Upserted {upserted_rows} rows into '{final_table}'.")

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
