import abc
from typing import Iterator, Dict, Any

class DatabaseLoader(abc.ABC):
    """
    Abstract base class for a database loader.

    This class defines the interface for database-specific loaders.
    Each loader implementation is responsible for connecting to the database,
    managing schemas, and efficiently loading data using native methods.
    """

    @abc.abstractmethod
    def connect(self, conn_str: str) -> None:
        """
        Establish a connection to the target database.

        :param conn_str: The database connection string.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_staging_schema(self, schema_name: str) -> None:
        """
        Ensure the staging schema exists.

        :param schema_name: The name of the schema for staging tables.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def bulk_load_native(self, table_name: str, data_iterator: Iterator[Dict[str, Any]], schema: Dict[str, str]) -> None:
        """
        Load data into a table using the database's native bulk loading mechanism.

        :param table_name: The name of the target table.
        :param data_iterator: An iterator yielding rows of data.
        :param schema: A dictionary representing the table schema.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute_merge_strategy(self, staging_table: str, final_table: str, primary_keys: list[str]) -> None:
        """
        Execute the SQL logic to merge data from a staging table to a final table.

        :param staging_table: The name of the staging table.
        :param final_table: The name of the final destination table.
        :param primary_keys: A list of primary key columns for the merge condition.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update_metadata(self, version: str, dataset: str, success: bool, row_count: int) -> None:
        """
        Record the outcome of a load operation in a metadata table.

        :param version: The Open Targets version.
        :param dataset: The name of the dataset loaded.
        :param success: Boolean indicating if the load was successful.
        :param row_count: The number of rows loaded.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def cleanup(self) -> None:
        """
        Perform cleanup operations, like closing connections or dropping temp tables.
        """
        raise NotImplementedError
