# FRD vs. Implementation Analysis: `py_load_opentargets`

This document provides a detailed comparison of the Functional Requirements Document (FRD) against the current implementation of the `py_load_opentargets` package.

## 1. Introduction & 2. Data Source Specifications
*   **Status:** Fully Met.
*   **Analysis:** The package is designed precisely for the purpose and scope outlined in the FRD. It correctly targets the specified audience (Data Engineers, Bioinformaticians) and is built with a clear understanding of the data source (Open Targets), its format (partitioned Parquet with nested structures), and its key datasets.

## 3. Core Functional Requirements

### 3.1 Data Acquisition and Versioning
*   **R.3.1.1: Filesystem Abstraction (`fsspec`)**
    *   **Status:** Fully Met.
    *   **Analysis:** The `data_acquisition.py` module uses `fsspec` for all remote file operations. This allows the package to seamlessly access data from any `fsspec`-compatible source (like GCS, FTP, S3, or local files) using a unified URI scheme.
    *   **Code Evidence (`src/py_load_opentargets/data_acquisition.py`):**
        ```python
        import fsspec

        def list_available_versions(discovery_uri: str) -> List[str]:
            # fsspec creates the correct filesystem object from the URI string
            fs, path = fsspec.core.url_to_fs(discovery_uri, anon=True)
            all_paths = fs.ls(path, detail=False)
            # ...
        ```

*   **R.3.1.2: Discover Latest Version**
    *   **Status:** Fully Met.
    *   **Analysis:** The `data_acquisition.list_available_versions` function discovers all available versions by listing directories that match a `YY.MM` pattern. It sorts them newest-to-oldest, making the first element the latest version. The CLI uses this function to automatically select the latest version if the user does not specify one.
    *   **Code Evidence (`src/py_load_opentargets/cli.py`):**
        ```python
        @cli.command()
        def load(ctx, version, ...):
            # ...
            if not version:
                click.echo("Discovering the latest version...")
                versions = list_available_versions(source_config['version_discovery_uri'])
                if not versions:
                    # handle error
                version = versions[0] # Selects the latest
                click.echo(f"Found latest version: {version}")
        ```

*   **R.3.1.3: Specify Specific Version**
    *   **Status:** Fully Met.
    *   **Analysis:** The `load` command in `cli.py` provides a `--version` option, allowing the user to explicitly define which Open Targets release to process.
    *   **Code Evidence (`src/py_load_opentargets/cli.py`):**
        ```python
        @click.option('--version', help='Open Targets version to load. Defaults to the latest.')
        def load(ctx, version, ...):
            # The 'version' variable is passed directly to the orchestrator
        ```

*   **R.3.1.4: Parallelized Downloading**
    *   **Status:** Fully Met.
    *   **Analysis:** The `data_acquisition.download_dataset` function uses a `concurrent.futures.ThreadPoolExecutor` to download multiple Parquet files for a given dataset in parallel. This significantly speeds up the data acquisition step. The number of parallel workers is configurable.
    *   **Code Evidence (`src/py_load_opentargets/data_acquisition.py`):**
        ```python
        def download_dataset(... max_workers: int = 1, ...):
            # ...
            if max_workers > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_file = {
                        executor.submit(...) for remote_file in remote_files
                    }
                    # ... process completed futures
        ```

*   **R.3.1.5: Data Integrity (Checksum Validation)**
    *   **Status:** Fully Met.
    *   **Analysis:** The implementation is very robust. The `ETLOrchestrator` first fetches a manifest of SHA1 checksums using `data_acquisition.get_checksum_manifest`. This manifest is then passed down and used to verify the integrity of every single file. The `stream` load strategy uses `verify_remote_dataset` to check files before processing, while the `download` strategy uses `_download_and_verify_one_file` to check them locally after download. The process fails immediately if a checksum is missing from the manifest or if a file's hash doesn't match.
    *   **Code Evidence (`src/py_load_opentargets/orchestrator.py`):**
        ```python
        # In ETLOrchestrator.run()
        self.checksum_manifest = get_checksum_manifest(
            self.version, checksum_uri_template
        )
        # ...
        # In ETLOrchestrator._process_dataset()
        verify_remote_dataset(
            remote_urls=parquet_uris,
            dataset=dataset_name,
            checksum_manifest=checksum_manifest,
            # ...
        )
        ```

### 3.2 Data Processing and Schema Management
*   **R.3.2.1: High-Performance Libraries**
    *   **Status:** Fully Met.
    *   **Analysis:** The package correctly uses `pyarrow` for schema operations and `polars` for high-performance, low-memory data transformation. The `_ParquetStreamer` class in `backends/postgres.py` uses Polars' lazy `scan_parquet` and `sink_csv` methods to process data in chunks and stream it directly to the database, avoiding loading large files into memory.
    *   **Code Evidence (`src/py_load_opentargets/backends/postgres.py`):**
        ```python
        import polars as pl
        # ...
        class _ParquetStreamer:
            # ...
            def _load_next_file_into_buffer(self):
                lf = pl.scan_parquet(next_path) # Polars lazy frame
                lf = lf.select(self._select_exprs) # Apply transformations
                lf.sink_csv(self._buffer, ...) # Stream result to an in-memory buffer
        ```

*   **R.3.2.2: Schema Inference and Mapping**
    *   **Status:** Fully Met.
    *   **Analysis:** The `PostgresLoader` infers the data schema from a Parquet file using `pyarrow.parquet.read_schema`. It then maps PyArrow types to appropriate PostgreSQL column types in the `_pyarrow_to_postgres_type` method. This mapping can be further customized for specific columns via the `schema_overrides` section in the configuration file.
    *   **Code Evidence (`src/py_load_opentargets/backends/postgres.py`):**
        ```python
        def _pyarrow_to_postgres_type(self, arrow_type: pa.DataType) -> str:
            if pa.types.is_integer(arrow_type):
                return "BIGINT"
            # ... etc
            elif pa.types.is_struct(arrow_type) or pa.types.is_list(arrow_type):
                return "JSONB" # Maps nested types to JSONB
        ```

*   **R.3.2.3: Schema Evolution**
    *   **Status:** Fully Met.
    *   **Analysis:** The `PostgresLoader.align_final_table_schema` method gracefully handles schema evolution. Before merging data, it compares the columns in the staging table (from the new data release) with the final table. If new columns are present in the source, it automatically issues `ALTER TABLE ... ADD COLUMN` statements to add them to the final table as nullable columns, preventing the load from failing.
    *   **Code Evidence (`src/py_load_opentargets/backends/postgres.py`):**
        ```python
        def align_final_table_schema(self, staging_table: str, final_table: str) -> None:
            # ...
            new_columns = staging_cols - final_cols
            if new_columns:
                logger.info(f"Found {len(new_columns)} new columns to add...")
                for col_name in sorted(list(new_columns)):
                    # ... executes "ALTER TABLE {final_table} ADD COLUMN {col_name} {col_type};"
        ```

*   **R.3.2.4: Handling Nested Data**
    *   **Status:** Fully Met.
    *   **Analysis:** The `PostgresLoader` provides powerful, configurable strategies for handling nested data via the `schema_overrides` configuration. A user can specify an `action` of `"flatten"` for struct columns (which unnests the fields into top-level columns) or `"json"` to serialize structs and lists into a single `JSONB` column. This transformation is handled efficiently on-the-fly by `polars` within the `_ParquetStreamer`.
    *   **Code Evidence (Example `config.toml`):**
        ```toml
        [datasets.evidence.schema_overrides.scores]
        action = "flatten" # Flattens the 'scores' struct

        [datasets.evidence.schema_overrides.urls]
        action = "json" # Serializes the 'urls' list to a JSONB column
        ```

### 3.3 Loading Strategies
*   **R.3.3.1.1: Full Load (Replace)**
    *   **Status:** Fully Met.
    *   **Analysis:** This is supported via the `--load-type full-refresh` CLI option. The `PostgresLoader.full_refresh_from_staging` method implements this by loading data into a staging table, dropping the old final table, and then efficiently renaming the staging table to become the new final table.
    *   **Code Evidence (`src/py_load_opentargets/orchestrator.py`):**
        ```python
        if self.load_type == "full-refresh":
            logger.info(f"Using 'full-refresh' strategy for final table...")
            loader.full_refresh_from_staging(...)
        ```

*   **R.3.3.2.1-4: Delta Load (Incremental)**
    *   **Status:** Fully Met.
    *   **Analysis:** This is the default `delta` load type and is implemented robustly.
        1.  **Staging:** Data is always loaded into a temporary staging table first (`R.3.3.2.2`).
        2.  **Efficient SQL:** The `PostgresLoader.execute_merge_strategy` method uses a highly efficient `DELETE-then-UPSERT` pattern. It first runs a `DELETE` statement to remove rows from the final table that are no longer present in the new data source. Then, it uses a single `INSERT ... ON CONFLICT (primary_key) DO UPDATE` statement to insert new rows and update existing ones (`R.3.3.2.3`).
        3.  **Idempotency:** The `ETLOrchestrator` checks the `_ot_load_metadata` table using `loader.get_last_successful_version` before starting a load. If the current version has already been loaded successfully, it skips the process, ensuring idempotency (`R.3.3.2.4`).

    *   **Code Evidence (`src/py_load_opentargets/backends/postgres.py`):**
        ```python
        # Stage 1: Delete records no longer in the source
        delete_sql = """DELETE FROM {final_table} AS f WHERE NOT EXISTS (...)"""
        self.cursor.execute(delete_sql)

        # Stage 2: Upsert new and updated records
        upsert_sql = """
        INSERT INTO {final_table} (...) SELECT ... FROM {staging_table}
        ON CONFLICT ({pk_cols}) DO UPDATE SET ...;
        """
        self.cursor.execute(upsert_sql)
        ```

### 3.4 Database Loading Architecture
*   **R.3.4.1: Abstraction Layer**
    *   **Status:** Fully Met.
    *   **Analysis:** `src/py_load_opentargets/loader.py` defines an abstract base class `DatabaseLoader` using Python's `abc` module. It defines the complete interface (`connect`, `bulk_load_native`, `execute_merge_strategy`, etc.) that any concrete backend implementation must adhere to.
    *   **Code Evidence (`src/py_load_opentargets/loader.py`):**
        ```python
        class DatabaseLoader(abc.ABC):
            @abc.abstractmethod
            def connect(self, conn_str: str, ...) -> None:
                raise NotImplementedError

            @abc.abstractmethod
            def bulk_load_native(self, table_name: str, ...) -> int:
                raise NotImplementedError
        ```

*   **R.3.4.2: Extensibility**
    *   **Status:** Fully Met.
    *   **Analysis:** The package uses Python's standard `entry_points` mechanism to create a plugin system for database backends. This is defined in `pyproject.toml` and used in `orchestrator.get_db_loader_factory` to discover and load the correct backend at runtime. This makes the system highly extensible without modifying the core code.
    *   **Code Evidence (`pyproject.toml`):**
        ```toml
        [project.entry-points."py_load_opentargets.backends"]
        postgres = "py_load_opentargets.backends.postgres:PostgresLoader"
        ```

*   **R.3.4.3: PostgreSQL Implementation (Default)**
    *   **Status:** Fully Met.
    *   **Analysis:** The default `PostgresLoader` is a best-practice implementation.
        *   (`R.3.4.3.2`) It uses the PostgreSQL `COPY` command via `psycopg.cursor.copy()`. Data is streamed from the `_ParquetStreamer` directly to the database, which is the most performant method possible as it minimizes memory usage and network overhead.
        *   (`R.3.4.3.3`) The `ETLOrchestrator` is designed to improve performance by calling the loader's methods to get, drop, and later recreate indexes and foreign keys around the main data merge operation.
    *   **Code Evidence (`src/py_load_opentargets/backends/postgres.py`):**
        ```python
        def bulk_load_native(...):
            copy_sql = f"COPY {table_name} (...) FROM STDIN WITH (...)"
            streamer = _ParquetStreamer(...)
            with self.cursor.copy(copy_sql) as copy:
                while chunk := streamer.read(8192): # Reads from the streamer
                    copy.write(chunk) # Writes directly to the COPY stream
        ```

### 3.5 Execution and Configuration
*   **R.3.5.1: Command Line Interface (CLI)**
    *   **Status:** Fully Met.
    *   **Analysis:** `src/py_load_opentargets/cli.py` implements a comprehensive CLI using the `click` library. It provides a `load` command with numerous options, as well as utility commands like `validate`, `list-versions`, and `discover-datasets`.

*   **R.3.5.2: Programmatic Python API**
    *   **Status:** **Not Met.**
    *   **Analysis:** This is the only significant gap in the implementation. While the internal components (`ETLOrchestrator`) can be imported and used, there is no simple, high-level function designed to be a stable, public API for developers. The primary entry point is the CLI, and the core orchestration logic is currently embedded within the `load` command's function.

*   **R.3.5.3: Configuration Management**
    *   **Status:** Fully Met.
    *   **Analysis:** The package uses a `config.py` module to load settings from a `default_config.toml`, which is merged with a user-provided `config.toml`. Sensitive information like the database connection string is correctly handled via an environment variable (`DB_CONN_STR`), which is a security best practice.

### 3.6 Metadata, Logging, and Monitoring
*   **R.3.6.1 & R.3.6.2: Metadata Schema**
    *   **Status:** Fully Met.
    *   **Analysis:** The `PostgresLoader` automatically creates and manages a `_ot_load_metadata` table. Its `update_metadata` method diligently records the Open Targets version, dataset name, start/end times, row counts, and success/failure status (including error messages) for every load attempt.

*   **R.3.6.3: Structured Logging**
    *   **Status:** Fully Met.
    *   **Analysis:** The package includes a `logging_utils.py` module and uses the `python-json-logger` library. The `--json-logs` CLI flag or a setting in the config file can enable structured JSON logging, which is ideal for ingestion into modern monitoring platforms.

## 4. Non-Functional Requirements & 5. Package Development
*   **Status:** Fully Met.
*   **Analysis:** The project structure and files indicate that all non-functional requirements are met.
    *   **Performance (`NFR.4.1`):** The use of streaming, `polars`, and native `COPY` demonstrates a clear focus on high-throughput performance.
    *   **Reliability & Idempotency (`NFR.4.2`):** The process is designed to be idempotent and resilient through metadata tracking and checksums.
    *   **Security (`NFR.4.3`):** Handled correctly via environment variables for credentials.
    *   **Packaging (`NFR.5.1`):** The project uses `pyproject.toml` with the `hatchling` backend, conforming to modern standards.
    *   **Testing (`NFR.5.2`):** The `tests/` directory is extensive, and the `pyproject.toml` specifies `pytest` and `testcontainers`, indicating a robust testing strategy.
    *   **CI/CD (`NFR.5.3`):** A `.github/workflows/ci.yml` file is present, indicating CI is in place.
    *   **Documentation (`NFR.5.4`):** A `docs/` directory with `mkdocs.yml` is present, showing that a documentation site is part of the project.

## Conclusion

The `py_load_opentargets` package is a mature, robust, and feature-rich tool that meets **almost all** of the specified functional and non-functional requirements. The implementation is of high quality, demonstrating a deep understanding of the problem domain and adherence to modern software engineering best practices.

The **single most significant gap** is the lack of a formal, documented **programmatic API (`R.3.5.2`)**. Fulfilling this requirement would improve the package's reusability for other developers who wish to integrate this ETL logic into their own applications. All other core requirements are fully and effectively implemented.
