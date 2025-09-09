import logging
from typing import List, Optional

from .config import load_config
from .data_acquisition import list_available_versions
from .orchestrator import ETLOrchestrator
from .validator import ValidationService
from .logging_utils import setup_logging

logger = logging.getLogger(__name__)


def load_opentargets(
    version: Optional[str] = None,
    datasets: Optional[List[str]] = None,
    config_path: Optional[str] = None,
    staging_schema: str = "staging",
    final_schema: str = "public",
    skip_confirmation: bool = False,
    continue_on_error: bool = True,
    load_type: str = "delta",
    json_logs: bool = False,
):
    """
    Programmatic entry point for the Open Targets ETL process.

    This function orchestrates the full ETL process for specified Open Targets
    datasets, from data acquisition to loading into a database. It serves as the
    main Python API for the package.

    Note: This function requires the `DB_CONN_STR` environment variable to be set
    with the target database connection string.

    Args:
        version (Optional[str]): The Open Targets release version to load (e.g., "22.04").
            If None, the latest available version will be discovered and used. Defaults to None.
        datasets (Optional[List[str]]): A list of specific dataset names to process.
            If None, all datasets defined in the configuration file will be processed.
            Defaults to None.
        config_path (Optional[str]): Path to a custom `config.toml` file. If None, the
            default packaged configuration is used. Defaults to None.
        staging_schema (str): The name of the database schema to use for staging tables.
            Defaults to "staging".
        final_schema (str): The name of the database schema for the final, merged tables.
            Defaults to "public".
        skip_confirmation (bool): If True, skips the confirmation prompt when a version
            has already been loaded. Defaults to False.
        continue_on_error (bool): If True, the process will continue with other datasets
            if one fails. If False, it will abort on the first error. Defaults to True.
        load_type (str): The strategy for loading data. Can be "delta" (default) to
            merge changes, or "full-refresh" to replace the table entirely.
        json_logs (bool): If True, configures logging to output in JSON format.
            Defaults to False.
    """
    # --- 1. Setup: Logging and Configuration ---
    # Load config first to check for logging settings within the file.
    try:
        config = load_config(config_path)
        use_json_logging = json_logs or config.get("logging", {}).get("json_format", False)
        setup_logging(json_format=use_json_logging)

        logger.info("--- Starting Open Targets ETL Process via Programmatic API ---")
        source_config = config["source"]
        all_defined_datasets = config["datasets"]
        datasets_to_process = datasets or list(all_defined_datasets.keys())

        logger.info(f"Selected datasets: {', '.join(datasets_to_process)}")
        logger.info(f"Load type: {load_type}")
        logger.info(f"Staging schema: '{staging_schema}', Final schema: '{final_schema}'")

        # --- 2. Validation ---
        logger.info("Validating configuration and connections...")
        validator = ValidationService(config)
        results = validator.run_all_checks()
        all_successful = True
        for check_name, result in results.items():
            if not result["success"]:
                all_successful = False
                message = result["message"]
                logger.error(f"Validation FAILED for '{check_name}': {message}")

        if not all_successful:
            logger.critical("Prerequisite validation failed. Please check your configuration.")
            # Raising an exception is more idiomatic for an API than aborting.
            raise RuntimeError("Prerequisite validation failed. Please check logs for details.")
        else:
            logger.info("Validation successful.")

        # --- 3. Version Discovery ---
        if not version:
            logger.info("No version specified, discovering the latest...")
            try:
                versions = list_available_versions(source_config["version_discovery_uri"])
                if not versions:
                    raise RuntimeError("Could not find any available Open Targets versions.")
                version = versions[0]
                logger.info(f"Found latest version: {version}")
            except Exception as e:
                logger.critical(f"A fatal error occurred during version discovery: {e}")
                raise  # Re-raise the exception to halt execution

        # --- 4. Orchestration & Execution ---
        logger.info(f"Preparing to load Open Targets version '{version}'.")
        orchestrator = ETLOrchestrator(
            config=config,
            datasets_to_process=datasets_to_process,
            version=version,
            staging_schema=staging_schema,
            final_schema=final_schema,
            skip_confirmation=skip_confirmation,
            continue_on_error=continue_on_error,
            load_type=load_type,
        )
        orchestrator.run()

    except Exception as e:
        logger.critical(f"A fatal error occurred during the ETL process: {e}", exc_info=True)
        # Re-raise the exception so the caller can handle it.
        raise
    finally:
        logger.info("--- Programmatic API Process Finished ---")
