import logging
import tempfile
from pathlib import Path
from typing import List, Dict, Any

from .loader import DatabaseLoader
from .data_acquisition import download_dataset

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """
    Orchestrates the end-to-end ETL process for Open Targets datasets.

    This class is responsible for managing the workflow of downloading,
    staging, and loading data for a list of specified datasets. It uses a
    database loader for all database interactions.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        loader: DatabaseLoader,
        datasets_to_process: List[str],
        version: str,
        staging_schema: str,
        final_schema: str,
        skip_confirmation: bool = False,
        continue_on_error: bool = True,
    ):
        """
        Initializes the ETLOrchestrator.

        :param config: The application configuration dictionary.
        :param loader: An instance of a DatabaseLoader subclass (e.g., PostgresLoader).
        :param datasets_to_process: A list of dataset names to process.
        :param version: The Open Targets version string to load.
        :param staging_schema: The name of the database schema for staging tables.
        :param final_schema: The name of the database schema for final tables.
        :param skip_confirmation: If True, skips user prompts for overwriting data.
        :param continue_on_error: If True, continue processing other datasets if one fails.
        """
        self.config = config
        self.loader = loader
        self.datasets_to_process = datasets_to_process
        self.version = version
        self.staging_schema = staging_schema
        self.final_schema = final_schema
        self.skip_confirmation = skip_confirmation
        self.continue_on_error = continue_on_error

    def run(self):
        """
        Executes the main ETL workflow for all specified datasets.
        """
        logger.info("--- Starting Open Targets ETL Process ---")
        logger.info(f"Selected datasets: {', '.join(self.datasets_to_process)}")

        all_defined_datasets = self.config['datasets']
        source_config = self.config['source']

        for dataset_name in self.datasets_to_process:
            if dataset_name not in all_defined_datasets:
                logger.warning(f"Dataset '{dataset_name}' is not defined in the configuration. Skipping.")
                continue

            dataset_config = all_defined_datasets[dataset_name]
            primary_keys = dataset_config['primary_key']
            final_table = dataset_config.get('final_table_name', dataset_name.replace('-', '_'))
            final_table_full_name = f"{self.final_schema}.{final_table}"
            staging_table_name = f"{self.staging_schema}.{dataset_name.replace('-', '_')}"

            logger.info("\n" + "=" * 80)
            logger.info(f"Processing dataset: {dataset_name}")
            logger.info(f"  - Version: {self.version}")
            logger.info(f"  - Primary Keys: {primary_keys}")
            logger.info(f"  - Staging Table: {staging_table_name}")
            logger.info(f"  - Final Table: {final_table_full_name}")
            logger.info("=" * 80)

            row_count = 0
            try:
                # Idempotency Check
                last_successful_version = self.loader.get_last_successful_version(dataset_name)
                if last_successful_version == self.version and not self.skip_confirmation:
                    logger.warning(f"Version '{self.version}' of '{dataset_name}' already loaded. Skipping as per config.")
                    continue

                with tempfile.TemporaryDirectory() as temp_dir_str:
                    temp_dir = Path(temp_dir_str)
                    logger.info(f"Downloading data to {temp_dir}...")
                    parquet_path = download_dataset(
                        source_config['data_download_uri_template'], self.version, dataset_name, temp_dir
                    )

                    logger.info("Preparing staging schema and table...")
                    self.loader.prepare_staging_schema(self.staging_schema)
                    self.loader.prepare_staging_table(staging_table_name, parquet_path)

                    logger.info(f"Bulk loading into staging table '{staging_table_name}'...")
                    row_count = self.loader.bulk_load_native(staging_table_name, parquet_path)
                    logger.info(f"Loaded {row_count} rows into staging.")

                    # Manage schema, indexes, and merge
                    indexes = []
                    try:
                        if self.loader.table_exists(final_table_full_name):
                            logger.info("Final table exists. Preparing for merge.")
                            logger.info("Aligning schema of final table...")
                            self.loader.align_final_table_schema(staging_table_name, final_table_full_name)

                            logger.info("Managing indexes for merge performance...")
                            indexes = self.loader.get_table_indexes(final_table_full_name)
                            if indexes:
                                self.loader.drop_indexes(indexes)

                        logger.info(f"Merging data into final table '{final_table_full_name}'...")
                        self.loader.execute_merge_strategy(staging_table_name, final_table_full_name, primary_keys)

                    finally:
                        # Always try to recreate indexes, even if merge fails
                        if indexes:
                            self.loader.recreate_indexes(indexes)

                    self.loader.update_metadata(version=self.version, dataset=dataset_name, success=True, row_count=row_count)
                    logger.info(f"Successfully merged {row_count} rows for dataset '{dataset_name}'.")

            except Exception as e:
                logger.error(f"Error processing dataset '{dataset_name}': {e}", exc_info=True)
                error_message = str(e).replace('\n', ' ').strip()
                self.loader.update_metadata(version=self.version, dataset=dataset_name, success=False, row_count=row_count, error_message=error_message)
                if not self.continue_on_error:
                    logger.error("Stopping execution due to error.")
                    raise  # Re-raise the exception to halt the entire process
                else:
                    logger.warning(f"Continuing to next dataset as per configuration.")

            finally:
                # Cleanup staging table for the processed dataset
                try:
                    logger.info(f"Dropping staging table '{staging_table_name}'...")
                    self.loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                    self.loader.conn.commit()
                except Exception as cleanup_e:
                    logger.warning(f"Failed to drop staging table '{staging_table_name}': {cleanup_e}")

        logger.info("\n--- Full Process Complete ---")
