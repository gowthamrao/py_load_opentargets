# Architecture

The `py-load-opentargets` package is designed to be modular, extensible, and performant. This document provides an overview of its internal architecture.

## Core Components

The system is built around a few key components that work together to orchestrate the ETL process.

### 1. Orchestrator (`orchestrator.py`)

The `ETLOrchestrator` is the main entry point and the brain of the loading process. Its key responsibilities are:

- **Configuration Loading**: Reads the `config.toml` file to understand what the user wants to do.
- **Dynamic Backend Loading**: It uses Python's `entry_points` mechanism to discover and load the correct database backend (e.g., `PostgresLoader`) based on the configuration. This makes the system pluggable.
- **Workflow Management**: It manages the end-to-end workflow for each dataset, including version checking, data acquisition, and database loading.
- **Parallel Execution**: It uses a `ThreadPoolExecutor` to process multiple datasets in parallel, significantly speeding up the overall process.
- **Error Handling**: It manages process-level error handling, with options to continue or halt on failure.

### 2. Data Acquisition (`data_acquisition.py`)

This module is responsible for all interactions with the remote Open Targets data sources.

- **Filesystem Abstraction**: It uses `fsspec` to communicate with data sources like Google Cloud Storage (GCS) and FTP, allowing for a unified interface.
- **Version Discovery**: It can automatically find the latest available data release by listing directories at the source.
- **Data Integrity**: It downloads the checksum manifest provided with each release, verifies the manifest's integrity, and then verifies the checksum of every data file before processing. This guarantees that the source data is not corrupt.
- **Remote Operations**: It can infer the schema of remote Parquet files without downloading them completely, which is a major performance optimization.

### 3. Database Loader (`loader.py` and `backends/`)

This is the component responsible for all database interactions. The architecture is based on the **Strategy Pattern**.

- **`DatabaseLoader` (Abstract Base Class)**: Defined in `loader.py`, this abstract class specifies the contract that every database backend must adhere to. It defines methods like `connect`, `bulk_load_native`, `execute_merge_strategy`, etc.
- **Concrete Implementations**: Each supported database has a concrete implementation of the `DatabaseLoader`. The default is `PostgresLoader` in `backends/postgres.py`.

#### The PostgreSQL Backend (`backends/postgres.py`)

The `PostgresLoader` is a highly optimized implementation for PostgreSQL.
- **Native Bulk Loading**: It uses the `COPY` command, which is the fastest way to get data into PostgreSQL.
- **Streaming Ingestion**: It uses a custom `_ParquetStreamer` class that reads multiple Parquet files, transforms them on-the-fly (see below), and streams them as a single, continuous TSV stream directly into the `COPY` command. This is extremely memory-efficient as the entire dataset is never loaded into memory at once.
- **Handling Nested Data**: It has a configurable strategy for handling nested `struct` types from Parquet:
    - **Flattening**: Struct fields can be flattened into separate top-level columns (e.g., `drug.id` becomes `drug_id`).
    - **JSON Serialization**: Any remaining `struct` types or `list` types are automatically serialized into a `JSONB` column in the database.
- **Schema Evolution**: It automatically detects new columns in a data release and adds them to the target table using `ALTER TABLE`.
- **Efficient Merging**: It uses a `DELETE-then-UPSERT` strategy to efficiently merge new data, handle updates, and remove stale records.

## Extensibility: Adding a New Database Backend

The use of `entry_points` makes it possible to add support for a new database (e.g., DuckDB, Redshift) without modifying the core package. To do so, a developer would:

1.  Create a new Python package (e.g., `py-load-opentargets-duckdb`).
2.  In that package, create a new loader class that inherits from `py_load_opentargets.DatabaseLoader`.
3.  Implement all the abstract methods with logic specific to the new database.
4.  In the new package's `pyproject.toml`, register the new loader class under the `py_load_opentargets.backends` entry point group.

Users could then install this new package alongside the core one and select the new backend in their `config.toml`.
