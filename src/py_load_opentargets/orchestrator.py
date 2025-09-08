import os
import logging
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib.metadata import entry_points

from .loader import DatabaseLoader
from .data_acquisition import download_dataset, get_checksum_manifest

logger = logging.getLogger(__name__)


def get_db_loader_factory(backend_name: str) -> Callable[[], DatabaseLoader]:
    """
    Dynamically creates a factory for a DatabaseLoader instance.
    This uses entry points to find the correct loader class.
    """
    logger.info(f"Looking for database backend '{backend_name}'...")
    try:
        # For Python 3.10+
        eps = entry_points(group='py_load_opentargets.backends')
    except TypeError:
        # Fallback for Python < 3.10
        eps = entry_points()['py_load_opentargets.backends']

    for ep in eps:
        if ep.name == backend_name:
            logger.info(f"Found backend entry point: {ep.name}")
            return ep.load
    raise ValueError(f"No registered backend found for '{backend_name}'")


class ETLOrchestrator:
    """
    Orchestrates the end-to-end ETL process for Open Targets datasets.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        datasets_to_process: List[str],
        version: str,
        staging_schema: str,
        final_schema: str,
        skip_confirmation: bool = False,
        continue_on_error: bool = True,
    ):
        self.config = config
        self.datasets_to_process = datasets_to_process
        self.version = version
        self.staging_schema = staging_schema
        self.final_schema = final_schema
        self.skip_confirmation = skip_confirmation
        self.continue_on_error = continue_on_error
        self.checksum_manifest = {}

        db_backend = self.config.get('database', {}).get('backend', 'postgres')
        self.loader_factory = get_db_loader_factory(db_backend)

    def _process_dataset(
        self,
        dataset_name: str,
        db_conn_str: str,
        max_workers: int,
        checksum_manifest: Dict[str, str],
    ):
        """
        Processes a single dataset. Designed to be called in a separate thread.
        It creates its own database loader and connection to ensure thread safety.
        """
        loader = self.loader_factory()
        try:
            loader.connect(db_conn_str)
            all_defined_datasets = self.config['datasets']
            source_config = self.config['source']

            dataset_config = all_defined_datasets[dataset_name]
            primary_keys = dataset_config['primary_key']
            final_table = dataset_config.get('final_table_name', dataset_name.replace('-', '_'))
            final_table_full_name = f"{self.final_schema}.{final_table}"
            staging_table_name = f"{self.staging_schema}.{dataset_name.replace('-', '_')}"

            logger.info(f"Processing dataset: {dataset_name} in thread")

            last_successful_version = loader.get_last_successful_version(dataset_name)
            if last_successful_version == self.version and not self.skip_confirmation:
                logger.warning(f"Version '{self.version}' of '{dataset_name}' already loaded. Skipping.")
                return f"Skipped: {dataset_name} version {self.version} already loaded."

            with tempfile.TemporaryDirectory() as temp_dir_str:
                temp_dir = Path(temp_dir_str)
                parquet_path = download_dataset(
                    source_config['data_download_uri_template'],
                    self.version,
                    dataset_name,
                    temp_dir,
                    checksum_manifest,
                    max_workers=max_workers,
                )
                loader.prepare_staging_schema(self.staging_schema)
                loader.prepare_staging_table(staging_table_name, parquet_path)
                row_count = loader.bulk_load_native(staging_table_name, parquet_path)

                indexes = []
                try:
                    if loader.table_exists(final_table_full_name):
                        loader.align_final_table_schema(staging_table_name, final_table_full_name)
                        indexes = loader.get_table_indexes(final_table_full_name)
                        if indexes: loader.drop_indexes(indexes)
                    loader.execute_merge_strategy(staging_table_name, final_table_full_name, primary_keys)
                finally:
                    if indexes: loader.recreate_indexes(indexes)

                loader.update_metadata(version=self.version, dataset=dataset_name, success=True, row_count=row_count)
                logger.info(f"Successfully processed dataset '{dataset_name}'.")
                return f"Success: {dataset_name}"
        except Exception as e:
            logger.error(f"Error processing dataset '{dataset_name}': {e}", exc_info=True)
            error_message = str(e).replace('\n', ' ').strip()
            loader.update_metadata(version=self.version, dataset=dataset_name, success=False, row_count=0, error_message=error_message)
            if not self.continue_on_error:
                raise
            return f"Failed: {dataset_name}"
        finally:
            if loader and loader.conn:
                try:
                    staging_table_name = f"{self.staging_schema}.{dataset_name.replace('-', '_')}"
                    loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                    loader.conn.commit()
                except Exception as cleanup_e:
                    logger.warning(f"Failed to drop staging table in worker: {cleanup_e}")
                loader.cleanup()

    def run(self):
        """
        Executes the main ETL workflow in parallel for all specified datasets.
        """
        logger.info("--- Starting Open Targets ETL Process ---")
        logger.info(f"Selected datasets: {', '.join(self.datasets_to_process)}")

        source_config = self.config['source']
        max_workers = self.config.get('execution', {}).get('max_workers', 1)
        db_conn_str = os.getenv("DB_CONN_STR")

        if not db_conn_str:
            logger.error("DB_CONN_STR environment variable not set.")
            raise ValueError("Database connection string is required.")

        # Fetch checksum manifest before starting any downloads
        try:
            checksum_uri_template = source_config.get('checksum_uri_template')
            if checksum_uri_template:
                self.checksum_manifest = get_checksum_manifest(self.version, checksum_uri_template)
            else:
                logger.warning("No 'checksum_uri_template' found in config. Skipping checksum validation.")
        except Exception as e:
            logger.error(f"Failed to retrieve checksum manifest. Halting process. Error: {e}")
            return

        if max_workers > 1:
            logger.info(f"Running with up to {max_workers} parallel workers.")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_dataset = {
                    executor.submit(
                        self._process_dataset,
                        name,
                        db_conn_str,
                        max_workers,
                        self.checksum_manifest,
                    ): name
                    for name in self.datasets_to_process
                }
                for future in as_completed(future_to_dataset):
                    dataset_name = future_to_dataset[future]
                    try:
                        result = future.result()
                        logger.info(f"Result for {dataset_name}: {result}")
                    except Exception as e:
                        logger.error(f"Dataset '{dataset_name}' generated an unhandled exception: {e}", exc_info=True)
                        if not self.continue_on_error:
                            logger.error("Halting due to error in worker.")
                            return
        else:
            logger.info("Running sequentially with a single worker.")
            for dataset in self.datasets_to_process:
                self._process_dataset(dataset, db_conn_str, max_workers, self.checksum_manifest)

        logger.info("\n--- Full Process Complete ---")
