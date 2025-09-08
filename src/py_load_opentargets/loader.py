import abc
from pathlib import Path
from typing import Optional, List, Dict


class DatabaseLoader(abc.ABC):
    """
    Abstract base class for a database loader.

    This class defines the complete interface required to implement a loader
    for a specific database backend. It covers connection, cleanup, metadata
    tracking, schema management, and the core ETL steps.
    """

    @abc.abstractmethod
    def connect(self, conn_str: str) -> None:
        """
        Establish a connection to the target database.

        :param conn_str: The database connection string.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def cleanup(self) -> None:
        """
        Perform cleanup operations, like closing connections.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_last_successful_version(self, dataset: str) -> Optional[str]:
        """
        Retrieves the version string of the last successful load for a given dataset.

        :param dataset: The name of the dataset.
        :return: The version string or None if no successful load is found.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update_metadata(self, version: str, dataset: str, success: bool, row_count: int, error_message: Optional[str] = None) -> None:
        """
        Record the outcome of a load operation in a metadata table.

        :param version: The Open Targets version.
        :param dataset: The name of the dataset loaded.
        :param success: Boolean indicating if the load was successful.
        :param row_count: The number of rows loaded/processed.
        :param error_message: An optional error message if the load failed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_staging_schema(self, schema_name: str) -> None:
        """
        Ensure the staging schema exists in the database.

        :param schema_name: The name of the schema for staging tables.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_staging_table(self, table_name: str, parquet_path: Path) -> None:
        """
        Create a staging table with a schema inferred from the provided Parquet file.
        The implementation should ensure the table is clean before creation.

        :param table_name: The fully qualified name of the staging table.
        :param parquet_path: Path to the directory of Parquet files for schema inference.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def bulk_load_native(self, table_name: str, parquet_path: Path) -> int:
        """
        Load data from a directory of Parquet files into a table using the
        database's most efficient, native bulk loading mechanism.

        :param table_name: The name of the target staging table.
        :param parquet_path: The path to the directory containing Parquet files.
        :return: The total number of rows loaded.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        :param table_name: The fully qualified name of the table (e.g., 'schema.table').
        :return: True if the table exists, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def align_final_table_schema(self, staging_table: str, final_table: str) -> None:
        """
        Align the schema of the final table to match the staging table by adding
        any missing columns.

        :param staging_table: The name of the staging table (source schema).
        :param final_table: The name of the final table (target schema).
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_table_indexes(self, table_name: str) -> List[Dict[str, str]]:
        """
        Retrieve definitions for all non-primary key indexes on a table.

        :param table_name: The fully qualified name of the table.
        :return: A list of dicts, where each dict has 'name' and 'ddl' for an index.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def drop_indexes(self, indexes: List[Dict[str, str]]) -> None:
        """
        Drops a list of indexes, typically before a large data modification.

        :param indexes: A list of index definition dicts, e.g., from get_table_indexes.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def recreate_indexes(self, indexes: List[Dict[str, str]]) -> None:
        """
        Recreates a list of indexes from their DDL definitions.

        :param indexes: A list of index definition dicts, e.g., from get_table_indexes.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute_merge_strategy(self, staging_table: str, final_table: str, primary_keys: List[str]) -> None:
        """
        Execute the SQL logic to merge data from a staging table to a final table.
        This should handle both initial creation of the final table and subsequent
        updates (UPSERT/MERGE).

        :param staging_table: The name of the staging table.
        :param final_table: The name of the final destination table.
        :param primary_keys: A list of primary key columns for the merge condition.
        """
        raise NotImplementedError
