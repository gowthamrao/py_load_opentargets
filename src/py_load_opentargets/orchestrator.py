import os
import logging
import tempfile
import time
from pathlib import Path
from typing import List, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib.metadata import entry_points

from .loader import DatabaseLoader
from .data_acquisition import (
    download_dataset,
    get_checksum_manifest,
    get_remote_dataset_urls,
    get_remote_schema,
    verify_remote_dataset,
)

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
            return ep.load()
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
        load_type: str = "delta",
    ):
        self.config = config
        self.datasets_to_process = datasets_to_process
        self.version = version
        self.staging_schema = staging_schema
        self.final_schema = final_schema
        self.skip_confirmation = skip_confirmation
        self.continue_on_error = continue_on_error
        self.load_type = load_type
        self.checksum_manifest = {}

        db_backend = self.config.get('database', {}).get('backend', 'postgres')
        self.loader_factory = get_db_loader_factory(db_backend)

    def _process_dataset(
        self,
        dataset_name: str,
        db_conn_str: str,
        max_workers: int,
        checksum_manifest: Dict[str, str],
        load_strategy: str,
    ):
        """
        Processes a single dataset. Designed to be called in a separate thread.
        It creates its own database loader and connection to ensure thread safety.
        """
        loader = self.loader_factory()
        start_time = time.time()
        end_time = None
        try:
            # Get configs
            all_defined_datasets = self.config["datasets"]
            source_config = self.config["source"]
            database_config = self.config.get("database", {})
            dataset_config = all_defined_datasets[dataset_name]

            # Combine database and dataset config to pass to loader
            # This allows the loader to access db-level settings like `flatten_separator`
            # and dataset-level settings like `flatten_structs`.
            loader_config = database_config.copy()
            loader_config.update(dataset_config)

            loader.connect(db_conn_str, loader_config)
            primary_keys = dataset_config["primary_key"]
            final_table = dataset_config.get(
                "final_table_name", dataset_name.replace("-", "_")
            )
            final_table_full_name = f"{self.final_schema}.{final_table}"
            staging_table_name = f"{self.staging_schema}.{dataset_name.replace('-', '_')}"

            logger.info(f"Processing dataset: {dataset_name} in thread")

            last_successful_version = loader.get_last_successful_version(dataset_name)
            if last_successful_version == self.version and not self.skip_confirmation:
                logger.warning(
                    f"Version '{self.version}' of '{dataset_name}' already loaded. Skipping."
                )
                return f"Skipped: {dataset_name} version {self.version} already loaded."

            loader.prepare_staging_schema(self.staging_schema)

            if load_strategy == "stream":
                uri_template = source_config["data_uri_template"]
                logger.info(f"Using 'stream' strategy for dataset '{dataset_name}'.")
                parquet_urls = get_remote_dataset_urls(
                    uri_template, self.version, dataset_name
                )
                if not parquet_urls:
                    logger.warning(f"No remote files found for {dataset_name}, skipping.")
                    return f"Skipped: {dataset_name} - No files found."

                # Verify checksums for all remote files before processing.
                verify_remote_dataset(
                    remote_urls=parquet_urls,
                    dataset=dataset_name,
                    checksum_manifest=checksum_manifest,
                    max_workers=max_workers,
                )

                schema = get_remote_schema(parquet_urls)
                loader.prepare_staging_table(staging_table_name, schema)
                row_count = loader.bulk_load_native(
                    staging_table_name, parquet_urls, schema
                )

            else: # 'download' strategy
                logger.info(f"Using 'download' strategy for dataset '{dataset_name}'.")
                with tempfile.TemporaryDirectory() as temp_dir_str:
                    temp_dir = Path(temp_dir_str)
                    download_uri_template = source_config['data_download_uri_template']
                    parquet_path = download_dataset(
                        download_uri_template,
                        self.version,
                        dataset_name,
                        temp_dir,
                        checksum_manifest,
                        max_workers=max_workers,
                    )

                    # Still use the new loader functions, but with local file paths
                    local_urls = [f"file://{p}" for p in sorted(parquet_path.glob("*.parquet"))]
                    if not local_urls:
                        logger.warning(f"No local files found for {dataset_name}, skipping.")
                        return f"Skipped: {dataset_name} - No files found."

                    schema = get_remote_schema(local_urls)
                    loader.prepare_staging_table(staging_table_name, schema)
                    row_count = loader.bulk_load_native(staging_table_name, local_urls, schema)

            # Based on the load type, choose the finalization strategy
            if self.load_type == "full-refresh":
                logger.info(f"Using 'full-refresh' strategy for final table '{final_table_full_name}'.")
                loader.full_refresh_from_staging(
                    staging_table_name, final_table_full_name, primary_keys
                )
            else:
                logger.info(f"Using 'delta' merge strategy for final table '{final_table_full_name}'.")
                indexes = []
                foreign_keys = []
                try:
                    if loader.table_exists(final_table_full_name):
                        loader.align_final_table_schema(
                            staging_table_name, final_table_full_name
                        )
                        # Get and drop FKs first
                        foreign_keys = loader.get_foreign_keys(final_table_full_name)
                        if foreign_keys:
                            loader.drop_foreign_keys(final_table_full_name, foreign_keys)

                        # Get and drop indexes
                        indexes = loader.get_table_indexes(final_table_full_name)
                        if indexes:
                            loader.drop_indexes(indexes)

                    loader.execute_merge_strategy(
                        staging_table_name, final_table_full_name, primary_keys
                    )
                finally:
                    # Recreate constraints in reverse order: indexes, then FKs
                    if indexes:
                        loader.recreate_indexes(indexes)
                    if foreign_keys:
                        loader.recreate_foreign_keys(final_table_full_name, foreign_keys)

            end_time = time.time()
            loader.update_metadata(
                version=self.version,
                dataset=dataset_name,
                success=True,
                row_count=row_count,
                start_time=start_time,
                end_time=end_time,
            )
            logger.info(f"Successfully processed dataset '{dataset_name}'.")
            return f"Success: {dataset_name}"
        except Exception as e:
            end_time = time.time()
            logger.error(f"Error processing dataset '{dataset_name}': {e}", exc_info=True)
            error_message = str(e).replace('\n', ' ').strip()
            loader.update_metadata(version=self.version, dataset=dataset_name, success=False, row_count=0, start_time=start_time, end_time=end_time, error_message=error_message)
            if not self.continue_on_error:
                raise
            return f"Failed: {dataset_name}"
        finally:
            if 'loader' in locals() and loader:
                loader.cleanup()

    def run(self):
        """
        Executes the main ETL workflow in parallel for all specified datasets.
        """
        logger.info("--- Starting Open Targets ETL Process ---")
        logger.info(f"Selected datasets: {', '.join(self.datasets_to_process)}")

        source_config = self.config["source"]
        execution_config = self.config.get("execution", {})
        max_workers = execution_config.get("max_workers", 1)
        load_strategy = execution_config.get("load_strategy", "download")
        db_conn_str = os.getenv("DB_CONN_STR")

        logger.info(f"Using load strategy: '{load_strategy}'")

        if not db_conn_str:
            logger.error("DB_CONN_STR environment variable not set.")
            raise ValueError("Database connection string is required.")

        # Fetch checksum manifest before starting any downloads. This is mandatory.
        checksum_uri_template = source_config.get("checksum_uri_template")
        if not checksum_uri_template:
            logger.error(
                "Missing 'checksum_uri_template' in source config. "
                "This is required for data integrity validation."
            )
            raise ValueError("Missing 'checksum_uri_template' in source config.")

        try:
            logger.info("Fetching and verifying checksum manifest...")
            self.checksum_manifest = get_checksum_manifest(
                self.version, checksum_uri_template
            )
            logger.info("Checksum manifest successfully loaded.")
        except Exception as e:
            logger.error(
                f"Failed to retrieve or verify checksum manifest. Halting process. Error: {e}"
            )
            # Re-raise to ensure the process stops
            raise

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
                        load_strategy,
                    ): name
                    for name in self.datasets_to_process
                }
                for future in as_completed(future_to_dataset):
                    dataset_name = future_to_dataset[future]
                    try:
                        result = future.result()
                        logger.info(f"Result for {dataset_name}: {result}")
                    except Exception as e:
                        logger.error(
                            f"Dataset '{dataset_name}' generated an unhandled exception: {e}",
                            exc_info=True,
                        )
                        if not self.continue_on_error:
                            logger.error("Halting due to error in worker.")
                            return
        else:
            logger.info("Running sequentially with a single worker.")
            for dataset in self.datasets_to_process:
                self._process_dataset(
                    dataset, db_conn_str, max_workers, self.checksum_manifest, load_strategy
                )

        logger.info("\n--- Full Process Complete ---")
