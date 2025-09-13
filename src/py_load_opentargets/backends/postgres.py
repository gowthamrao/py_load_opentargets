import psycopg
from psycopg import sql
import pyarrow as pa
import polars as pl
import logging
import io
import json
from typing import Dict, Any, List, Optional

from ..loader import DatabaseLoader

logger = logging.getLogger(__name__)


class _ParquetStreamer:
    """
    A file-like object that streams multiple Parquet files as a single TSV stream.
    This class iterates through a list of Parquet file paths, processes them with
    Polars according to schema transformation rules, and streams them as a single,
    continuous, tab-delimited text stream suitable for PostgreSQL's COPY command.
    """

    def __init__(
        self,
        paths: List[str],
        original_schema: pa.Schema,
        schema_overrides: Optional[Dict[str, Any]],
        flatten_separator: str,
    ):
        self._paths = iter(paths)
        self._original_schema = original_schema
        self._schema_overrides = schema_overrides or {}
        self._flatten_separator = flatten_separator
        self._buffer = io.StringIO()
        self._eof = False
        self._select_exprs = self._build_polars_expressions()

    def _build_polars_expressions(self) -> List[pl.Expr]:
        """
        Builds a list of Polars expressions based on the original schema and schema_overrides.
        This is done once at initialization.
        """
        select_exprs = []
        for field in self._original_schema:
            col_name = field.name
            override = self._schema_overrides.get(col_name, {})
            action = override.get("action")
            final_name = override.get("rename", col_name)

            # Case 1: Flatten a struct
            if action == "flatten" and pa.types.is_struct(field.type):
                logger.info(
                    f"Rule: Flattening all fields from struct column '{col_name}'."
                )
                for sub_field in field.type:
                    new_col_name = (
                        f"{final_name}{self._flatten_separator}{sub_field.name}"
                    )
                    select_exprs.append(
                        pl.col(col_name)
                        .struct.field(sub_field.name)
                        .alias(new_col_name)
                    )
            # Case 2: Serialize to JSON
            elif (
                action == "json"
                or (pa.types.is_struct(field.type) and action != "flatten")
                or pa.types.is_list(field.type)
            ):
                logger.info(
                    f"Rule: Serializing column '{col_name}' to JSON as '{final_name}'."
                )
                # Use the appropriate json_encode method based on the type
                if pa.types.is_struct(field.type):
                    expr = pl.col(col_name).struct.json_encode()
                elif pa.types.is_list(field.type):
                    # For lists, map_elements passes a Series for each row. We must
                    # convert it to a list before JSON serialization.
                    # We use compact separators for efficiency.
                    expr = pl.col(col_name).map_elements(
                        lambda s: json.dumps(s.to_list(), separators=(",", ":")),
                        return_dtype=pl.String,
                    )
                else:
                    # Fallback for other types that might be marked as json
                    expr = pl.col(col_name).cast(pl.String)

                select_exprs.append(expr.alias(final_name))
            # Case 3: Simple rename or direct mapping
            else:
                logger.info(f"Rule: Mapping column '{col_name}' to '{final_name}'.")
                select_exprs.append(pl.col(col_name).alias(final_name))

        return select_exprs

    def _load_next_file_into_buffer(self):
        """Loads the next parquet file into the in-memory buffer."""
        try:
            next_path = next(self._paths)
            logger.info(f"Streaming file for COPY: {next_path}")

            lf = pl.scan_parquet(next_path)
            lf = lf.select(self._select_exprs)

            # Reset buffer and write new data into it
            self._buffer.seek(0)
            self._buffer.truncate(0)
            lf.sink_csv(
                self._buffer,
                separator="\t",
                null_value="\\N",
                include_header=False,
                quote_style="never",
            )
            self._buffer.seek(0)

        except StopIteration:
            logger.info("All files have been streamed.")
            self._eof = True

    def read(self, size: int = -1) -> str:
        """Read 'size' bytes from the stream."""
        if self._eof:
            return ""

        data = self._buffer.read(size)
        if not data and not self._eof:
            self._load_next_file_into_buffer()
            if self._eof:
                return ""
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
        self.dataset_config = {}

    def connect(
        self, conn_str: str, dataset_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """Establishes a connection to the PostgreSQL database."""
        logger.info("Connecting to PostgreSQL database...")
        self.dataset_config = dataset_config or {}
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
            return "BIGINT"
        elif pa.types.is_floating(arrow_type):
            return "DOUBLE PRECISION"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        elif pa.types.is_date(arrow_type):
            return "DATE"
        elif pa.types.is_struct(arrow_type) or pa.types.is_list(arrow_type):
            return "JSONB"
        else:
            logger.warning(
                f"Unsupported PyArrow type {arrow_type}. Defaulting to TEXT."
            )
            return "TEXT"

    def _get_transformed_schema(self, original_schema: pa.Schema) -> pa.Schema:
        """
        Transforms a PyArrow schema based on the rules in 'schema_overrides'.
        This determines the final schema of the database table.
        """
        schema_overrides = self.dataset_config.get("schema_overrides", {})
        flatten_separator = self.dataset_config.get("flatten_separator", "_")
        new_fields = []

        for field in original_schema:
            original_name = field.name
            override = schema_overrides.get(original_name, {})
            action = override.get("action")
            final_name = override.get("rename", original_name)

            # Case 1: Flatten a struct
            if action == "flatten" and pa.types.is_struct(field.type):
                for sub_field in field.type:
                    new_name = f"{final_name}{flatten_separator}{sub_field.name}"
                    new_fields.append(pa.field(new_name, sub_field.type))
            # Case 2: Serialize to JSON
            elif (
                action == "json"
                or (pa.types.is_struct(field.type) and action != "flatten")
                or pa.types.is_list(field.type)
            ):
                # Keep the original nested type, but with the new name.
                # _pyarrow_to_postgres_type will map this to JSONB.
                new_fields.append(field.with_name(final_name))
            # Case 3: Simple rename or direct mapping
            else:
                new_fields.append(field.with_name(final_name))

        return pa.schema(new_fields)

    def _generate_create_table_sql(
        self, table_name: str, schema: pa.Schema, schema_overrides: dict
    ) -> str:
        """Generates a CREATE TABLE SQL statement from a transformed PyArrow schema."""
        columns = []
        for field in schema:
            col_name = field.name
            # Check for a user-defined type override first
            override = schema_overrides.get(
                field.name, {}
            )  # Note: This lookup is on the final name
            col_type = override.get("type", self._pyarrow_to_postgres_type(field.type))
            columns.append(f'"{col_name}" {col_type}')

        cols_sql = ",\n  ".join(columns)
        return f"CREATE TABLE {table_name} (\n  {cols_sql}\n);"

    def prepare_staging_table(self, table_name: str, schema: pa.Schema) -> None:
        """
        Creates a staging table with a schema transformed based on the config.
        The table is dropped if it already exists.
        """
        logger.info(f"Preparing staging table '{table_name}'...")

        schema_overrides = self.dataset_config.get("schema_overrides", {})
        transformed_schema = self._get_transformed_schema(schema)
        create_sql = self._generate_create_table_sql(
            table_name, transformed_schema, schema_overrides
        )

        logger.info(f"Dropping table '{table_name}' if it exists.")
        self.cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name))
        )

        logger.info(f"Creating table '{table_name}' with transformed schema.")
        logger.debug(f"CREATE TABLE SQL:\n{create_sql}")
        self.cursor.execute(create_sql)
        self.conn.commit()

    def bulk_load_native(
        self, table_name: str, parquet_uris: List[str], schema: pa.Schema
    ) -> int:
        """
        Loads data from a list of Parquet files into a PostgreSQL table using a
        single, streamed, native COPY command, handling data transformations on the fly.
        """
        logger.info(f"Starting single-stream bulk load for '{table_name}'.")

        if not parquet_uris:
            logger.warning("No .parquet files provided. Skipping bulk load.")
            return 0

        schema_overrides = self.dataset_config.get("schema_overrides", {})
        flatten_separator = self.dataset_config.get("flatten_separator", "_")

        # The schema of the data stream must match the table schema
        transformed_schema = self._get_transformed_schema(schema)
        columns_str = ", ".join([f'"{f.name}"' for f in transformed_schema])
        copy_sql = f"COPY {table_name} ({columns_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"

        streamer = _ParquetStreamer(
            paths=parquet_uris,
            original_schema=schema,
            schema_overrides=schema_overrides,
            flatten_separator=flatten_separator,
        )

        logger.info(f"Executing single COPY command for {len(parquet_uris)} files...")
        with self.cursor.copy(copy_sql) as copy:
            while True:
                chunk = streamer.read(8192)
                if not chunk:
                    break
                copy.write(chunk)

        total_rows = self.cursor.rowcount
        self.conn.commit()

        logger.info(
            f"Total rows loaded into '{table_name}' in a single transaction: {total_rows}"
        )
        return total_rows

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
            (table_name,),
        )
        if self.cursor.fetchone()[0]:
            logger.info(f"Metadata table '{table_name}' already exists.")
            return

        logger.info(f"Metadata table '{table_name}' not found, creating it...")
        create_sql = f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            load_timestamp TIMESTAMPTZ DEFAULT NOW(),
            start_time TIMESTAMPTZ,
            end_time TIMESTAMPTZ,
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
            (dataset,),
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def table_exists(self, table_name: str) -> bool:
        """Checks if a table exists in the database."""
        try:
            schema, table = table_name.split(".")
        except ValueError:
            logger.error(
                f"Invalid table name format '{table_name}'. Expected 'schema.table'."
            )
            raise

        self.cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = %s AND table_name = %s);",
            (schema, table),
        )
        return self.cursor.fetchone()[0]

    def get_table_indexes(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieves definitions for all non-primary key indexes on a table.

        :param table_name: The fully qualified name of the table.
        :return: A list of dicts, where each dict has 'name' and 'ddl' for an index.
        """
        schema, table = table_name.split(".")
        logger.info(f"Retrieving index definitions for table '{table_name}'.")
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
            logger.info(
                f"Found {len(indexes)} non-PK indexes: {[i['name'] for i in indexes]}"
            )
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
            index_name = index["name"]
            logger.info(f"Dropping index: {index_name}")
            self.cursor.execute(
                sql.SQL("DROP INDEX IF EXISTS {};").format(sql.Identifier(index_name))
            )
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
            self.cursor.execute(index["ddl"])
        self.conn.commit()
        logger.info("All indexes recreated successfully.")

    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieves definitions for all foreign key constraints on a table.
        """
        schema, table = table_name.split(".")
        logger.info(f"Retrieving foreign key definitions for table '{table_name}'.")
        sql_query = """
            SELECT
                con.conname AS name,
                pg_get_constraintdef(con.oid) AS ddl
            FROM
                pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE
                nsp.nspname = %s AND rel.relname = %s AND con.contype = 'f';
        """
        self.cursor.execute(sql_query, (schema, table))
        foreign_keys = [
            {"name": row[0], "ddl": row[1]} for row in self.cursor.fetchall()
        ]
        if foreign_keys:
            logger.info(
                f"Found {len(foreign_keys)} foreign keys: {[fk['name'] for fk in foreign_keys]}"
            )
        else:
            logger.info("No foreign keys found.")
        return foreign_keys

    def drop_foreign_keys(
        self, table_name: str, foreign_keys: List[Dict[str, str]]
    ) -> None:
        """
        Drops a list of foreign key constraints from a table.
        """
        if not foreign_keys:
            return
        logger.info(
            f"Dropping {len(foreign_keys)} foreign keys from {table_name} to improve merge performance..."
        )
        table_ident = sql.Identifier(*table_name.split("."))
        for fk in foreign_keys:
            fk_name = fk["name"]
            logger.info(f"Dropping foreign key: {fk_name}")
            self.cursor.execute(
                sql.SQL(
                    "ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {fk_name};"
                ).format(table=table_ident, fk_name=sql.Identifier(fk_name))
            )
        self.conn.commit()

    def recreate_foreign_keys(
        self, table_name: str, foreign_keys: List[Dict[str, str]]
    ) -> None:
        """
        Recreates a list of foreign key constraints on a table from their DDL definitions.
        """
        if not foreign_keys:
            return
        logger.info(f"Recreating {len(foreign_keys)} foreign keys on {table_name}...")
        table_ident = sql.Identifier(*table_name.split("."))
        for fk in foreign_keys:
            logger.info(f"Recreating foreign key '{fk['name']}' using DDL: {fk['ddl']}")
            self.cursor.execute(
                sql.SQL("ALTER TABLE {table} ADD CONSTRAINT {fk_name} {ddl};").format(
                    table=table_ident,
                    fk_name=sql.Identifier(fk["name"]),
                    ddl=sql.SQL(fk["ddl"]),
                )
            )
        self.conn.commit()
        logger.info("All foreign keys recreated successfully.")

    def _get_table_columns(self, table_name: str) -> list[str]:
        """Retrieves a list of column names for a given table."""
        schema, table = table_name.split(".")
        self.cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position;",
            (schema, table),
        )
        return [row[0] for row in self.cursor.fetchall()]

    def _get_table_schema_from_db(self, table_name: str) -> Dict[str, str]:
        """
        Retrieves a dictionary of {column_name: data_type} for a given table.
        """
        schema, table = table_name.split(".")
        self.cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s;
            """,
            (schema, table),
        )
        return {row[0]: row[1] for row in self.cursor.fetchall()}

    def align_final_table_schema(self, staging_table: str, final_table: str) -> None:
        logger.info(
            f"Aligning schema of final table '{final_table}' with staging table '{staging_table}'."
        )
        staging_schema = self._get_table_schema_from_db(staging_table)
        final_schema = self._get_table_schema_from_db(final_table)
        staging_cols = set(staging_schema.keys())
        final_cols = set(final_schema.keys())
        new_columns = staging_cols - final_cols
        if new_columns:
            logger.info(
                f"Found {len(new_columns)} new columns to add: {', '.join(sorted(new_columns))}"
            )
            for col_name in sorted(list(new_columns)):
                col_type = staging_schema[col_name]
                logger.info(
                    f"Adding column '{col_name}' with type '{col_type}' to '{final_table}'."
                )
                alter_sql = sql.SQL(
                    "ALTER TABLE {final_table} ADD COLUMN {col_name} {col_type};"
                ).format(
                    final_table=sql.Identifier(*final_table.split(".")),
                    col_name=sql.Identifier(col_name),
                    col_type=sql.SQL(col_type),
                )
                self.cursor.execute(alter_sql)
            self.conn.commit()
            logger.info("Successfully added new columns.")
        else:
            logger.info("No new columns to add.")
        common_columns = staging_cols.intersection(final_cols)
        mismatched_columns = []
        for col_name in common_columns:
            staging_type = (
                staging_schema[col_name].lower().replace("character varying", "varchar")
            )
            final_type = (
                final_schema[col_name].lower().replace("character varying", "varchar")
            )
            if staging_type != final_type:
                mismatched_columns.append(col_name)
                logger.warning(
                    f"SCHEMA DRIFT DETECTED for column '{col_name}': "
                    f"Staging table type is '{staging_type}', but final table type is '{final_type}'. "
                    "The loader will not attempt to alter the column."
                )
        if not mismatched_columns:
            logger.info("No data type mismatches found in common columns.")
        removed_columns = final_cols - staging_cols
        if removed_columns:
            logger.warning(
                f"The following columns exist in the final table '{final_table}' but not in the "
                f"new data source: {', '.join(sorted(removed_columns))}. These columns will be "
                "kept with their existing data, but will be NULL for new/updated rows."
            )
        logger.info(f"Schema alignment check for '{final_table}' complete.")

    def execute_merge_strategy(
        self, staging_table: str, final_table: str, primary_keys: list[str]
    ) -> None:
        """
        Merges data from the staging table to the final table using a
        DELETE-then-UPSERT strategy to handle additions, updates, and deletions.

        If the final table does not exist, it is created by copying the staging table.
        """
        logger.info(f"Starting merge from '{staging_table}' to '{final_table}'.")

        final_table_ident = sql.Identifier(*final_table.split("."))
        staging_table_ident = sql.Identifier(*staging_table.split("."))
        pk_idents = [sql.Identifier(k) for k in primary_keys]

        # 1. Handle initial load: if final table doesn't exist, create it and copy data
        if not self.table_exists(final_table):
            logger.info(
                f"Final table '{final_table}' does not exist. Creating and copying data..."
            )
            self.cursor.execute(
                sql.SQL("CREATE TABLE {final} AS TABLE {staging};").format(
                    final=final_table_ident, staging=staging_table_ident
                )
            )
            pk_constraint_name = sql.Identifier(f"pk_{final_table.replace('.', '_')}")
            self.cursor.execute(
                sql.SQL(
                    "ALTER TABLE {final} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols});"
                ).format(
                    final=final_table_ident,
                    pk_name=pk_constraint_name,
                    pk_cols=sql.SQL(", ").join(pk_idents),
                )
            )
            logger.info(f"Successfully created and populated '{final_table}'.")
            self.conn.commit()
            return

        logger.info(
            f"Final table '{final_table}' exists. Performing DELETE-then-UPSERT."
        )

        # --- Stage 1: Delete records that are in the final table but not in the new staging data ---
        # This uses a `NOT EXISTS` subquery which is highly efficient in PostgreSQL.
        # It deletes any row from the final table if its primary key does not exist
        # in the staging table.
        logger.info("Identifying and deleting stale records from final table...")

        join_condition = sql.SQL(" AND ").join(
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
            join_condition=join_condition,
        )

        self.cursor.execute(delete_sql)
        deleted_rows = self.cursor.rowcount
        logger.info(f"Deleted {deleted_rows} stale records from '{final_table}'.")

        # --- Stage 2: Upsert records from staging into final ---
        all_columns = self._get_table_columns(staging_table)
        update_columns = [col for col in all_columns if col not in primary_keys]

        if not update_columns:
            logger.warning(
                f"No columns to update for table '{final_table}'. All columns are part of the primary key. Skipping UPSERT."
            )
            self.conn.commit()
            return

        all_cols_idents = [sql.Identifier(c) for c in all_columns]
        update_cols_idents = [sql.Identifier(c) for c in update_columns]
        update_clause = sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=col)
            for col in update_cols_idents
        )

        upsert_sql = sql.SQL("""
        INSERT INTO {final_table} ({all_cols})
        SELECT {all_cols} FROM {staging_table}
        ON CONFLICT ({pk_cols}) DO UPDATE SET
        {update_clause};
        """).format(
            final_table=final_table_ident,
            all_cols=sql.SQL(", ").join(all_cols_idents),
            staging_table=staging_table_ident,
            pk_cols=sql.SQL(", ").join(pk_idents),
            update_clause=update_clause,
        )

        logger.info("Executing UPSERT operation...")
        self.cursor.execute(upsert_sql)
        upserted_rows = self.cursor.rowcount
        logger.info(f"Upserted {upserted_rows} rows into '{final_table}'.")

        self.conn.commit()
        logger.info(f"Successfully merged data into '{final_table}'.")

    def update_metadata(
        self,
        version: str,
        dataset: str,
        success: bool,
        row_count: int,
        start_time: Optional[float],
        end_time: Optional[float],
        error_message: str = None,
    ) -> None:
        """
        Record the outcome of a load operation in the metadata table.
        """
        self._ensure_metadata_table_exists()
        status = "success" if success else "failure"
        duration = f"{end_time - start_time:.2f}s" if start_time and end_time else "N/A"
        logger.info(
            f"Updating metadata for dataset '{dataset}', version '{version}': "
            f"status={status}, rows_loaded={row_count}, duration={duration}"
        )

        insert_sql = """
        INSERT INTO _ot_load_metadata
        (opentargets_version, dataset_name, rows_loaded, status, error_message, start_time, end_time)
        VALUES (%s, %s, %s, %s, %s, to_timestamp(%s), to_timestamp(%s));
        """
        self.cursor.execute(
            insert_sql,
            (version, dataset, row_count, status, error_message, start_time, end_time),
        )
        self.conn.commit()

    def full_refresh_from_staging(
        self, staging_table: str, final_table: str, primary_keys: list[str]
    ) -> None:
        """
        Performs a full refresh by recreating the final table from the staging table.
        This approach is transactionally robust.
        """
        logger.info(
            f"Performing full refresh from '{staging_table}' to '{final_table}'."
        )

        final_table_ident = sql.Identifier(*final_table.split("."))
        staging_table_ident = sql.Identifier(*staging_table.split("."))
        pk_idents = [sql.Identifier(k) for k in primary_keys]
        pk_constraint_name = sql.Identifier(f"pk_{final_table.replace('.', '_')}")

        # Drop the old final table
        logger.info(f"Dropping final table '{final_table}' if it exists.")
        self.cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {final_table} CASCADE;").format(
                final_table=final_table_ident
            )
        )

        # Recreate the final table from the staging table
        logger.info(f"Creating new final table '{final_table}' from '{staging_table}'.")
        self.cursor.execute(
            sql.SQL("CREATE TABLE {final_table} AS TABLE {staging_table};").format(
                final_table=final_table_ident, staging_table=staging_table_ident
            )
        )

        # Add the primary key to the new final table
        logger.info(
            f"Adding primary key constraint '{pk_constraint_name}' to '{final_table}'."
        )
        self.cursor.execute(
            sql.SQL(
                "ALTER TABLE {final_table} ADD CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols});"
            ).format(
                final_table=final_table_ident,
                pk_name=pk_constraint_name,
                pk_cols=sql.SQL(", ").join(pk_idents),
            )
        )

        # Drop the now-obsolete staging table
        logger.info(f"Dropping staging table '{staging_table}'.")
        self.cursor.execute(
            sql.SQL("DROP TABLE {staging_table};").format(
                staging_table=staging_table_ident
            )
        )

        self.conn.commit()
        logger.info(f"Successfully completed full refresh for '{final_table}'.")
