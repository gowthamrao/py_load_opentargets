# Functional Requirements Document: py-load-opentargets

**Package Author:** Gowtham Rao <rao@ohdsi.org>
**Package Name:** `py-load-opentargets`
**Version:** 1.0

---

## 1. Introduction

### 1.1 Purpose and Scope

The purpose of the `py-load-opentargets` package is to provide a robust, efficient, and extensible command-line and programmatic tool for downloading and loading Open Targets platform data into various relational database systems.

**Scope:**
*   **In-Scope:**
    *   Discovering and downloading specific or latest versions of Open Targets data releases.
    *   Processing large, partitioned Parquet datasets from the release.
    *   Mapping and transforming data to fit a relational schema, including handling of nested structures.
    *   Efficiently loading the data into target databases using native bulk-loading mechanisms.
    *   Providing both full-load (replace) and incremental update (delta load) strategies.
    *   Supporting PostgreSQL as the default, primary database backend.
    *   A flexible architecture that allows for the addition of other database backends (e.g., Redshift, BigQuery, Databricks SQL).
    *   Comprehensive logging, monitoring, and metadata tracking of the loading process.
*   **Out-of-Scope:**
    *   Data analysis or querying capabilities. The package is focused solely on the ETL (Extract, Transform, Load) process.
    *   Creation of a specific data model or schema for Open Targets within the database. The package will load the data as-is, with minimal transformation, and the user is responsible for the target schema design.
    *   A graphical user interface (GUI).

### 1.2 Target Audience

*   **Data Engineers:** Professionals responsible for building and maintaining data pipelines who need to ingest Open Targets data into an analytical environment.
*   **Bioinformaticians & Computational Biologists:** Scientists who need local, performant access to Open Targets data within a relational database for research purposes.
*   **Software Developers:** Developers building applications that leverage Open Targets data who require a reliable method to populate their application's database.

### 1.3 Glossary of Terms

| Term | Definition |
| --- | --- |
| **Open Targets** | A public-private partnership that generates and disseminates evidence on the validity of therapeutic targets. |
| **Parquet** | A columnar storage file format optimized for use with big data processing frameworks. |
| **FTP** | File Transfer Protocol. One of the access methods for Open Targets data. |
| **GCS** | Google Cloud Storage. One of the access methods for Open Targets data. |
| **fsspec** | A Python library providing a unified interface to various local, remote, and cloud file systems. |
| **PyArrow** | A cross-language development platform for in-memory data, used for efficient Parquet file handling. |
| **Polars** | A high-performance DataFrame library for Python, built in Rust. |
| **ETL** | Extract, Transform, Load. A standard data integration process. |
| **Idempotency** | The property of an operation that ensures it can be applied multiple times without changing the result beyond the initial application. |
| **CLI** | Command Line Interface. |
| **API** | Application Programming Interface. |

---

## 2. Data Source Specifications

### 2.1 Data Origin and Access

The primary data source is the **Open Targets Platform**. Data releases are available for public download from:
*   **Website:** [https://platform.opentargets.org/downloads](https://platform.opentargets.org/downloads)
*   **FTP Server:** `ftp.ebi.ac.uk/pub/databases/opentargets/platform/...`
*   **Google Cloud Storage (GCS) Bucket:** `gs://open-targets/platform/...`

The package must be able to access data from any of these sources, with the specific location being a configurable parameter.

### 2.2 Data Format

The data is provided as a collection of datasets in **Apache Parquet** format.
*   **Partitioning:** Datasets are often partitioned (e.g., by data type or source) into multiple Parquet files within a directory structure.
*   **Nested Structures:** The Parquet files contain columns with nested data types (e.g., structs, lists of structs). The package must have a strategy to handle these structures.

### 2.3 Key Datasets

The package must be capable of loading all datasets provided in an Open Targets release. Key datasets include, but are not limited to:
*   `targets`
*   `diseases`
*   `molecule` (Drugs)
*   `evidence`
*   `associationByOverallDirect`
*   `associationByDatasourceDirect`

---

## 3. Core Functional Requirements

### 3.1 Data Acquisition and Versioning

*   **R.3.1.1:** The package **must** use a filesystem abstraction library, such as `fsspec`, to interact with data sources, allowing seamless switching between local paths, FTP, GCS, and potential future sources like S3.
*   **R.3.1.2:** The package **must** provide a mechanism to automatically discover the latest available Open Targets release version (e.g., by listing directories at the source).
*   **R.3.1.3:** Users **must** be able to specify a particular release version for download.
*   **R.3.1.4:** The download process **should** be parallelized to improve efficiency when fetching multiple files.
*   **R.3.1.5:** The package **must** verify the integrity of downloaded data. If checksums (e.g., MD5) are provided by Open Targets, they must be used for validation.

### 3.2 Data Processing and Schema Management

*   **R.3.2.1:** The package **must** use a high-performance library like PyArrow or Polars to read and process large, partitioned Parquet datasets with minimal memory overhead.
*   **R.3.2.2 (Schema Inference):** The package **must** automatically infer the data schema from each Parquet file. It must include a configurable mapping from Parquet data types (e.g., `string`, `int64`, `struct`) to appropriate SQL types (e.g., `VARCHAR`, `BIGINT`, `JSONB`).
*   **R.3.2.3 (Schema Evolution):** The package **must** gracefully handle schema changes between Open Targets releases. If a new column is added, the loader should be able to add the corresponding column to the target database table (as nullable) without failing.
*   **R.3.2.4 (Handling Nested Data):** The package **must** provide a configurable strategy for handling nested Parquet columns. Default strategies should include:
    *   Flattening specific, commonly used nested fields into top-level columns.
    *   Serializing complex or deeply nested structures into a `JSON` or `JSONB` column in the target database.

### 3.3 Loading Strategies

*   **3.3.1 Full Load (Replace)**
    *   **R.3.3.1.1:** The package **must** offer a "full load" mode that completely replaces existing data. This will be implemented by truncating the target tables before initiating the new data load.

*   **3.3.2 Delta Load (Incremental)**
    *   **R.3.3.2.1:** The package **must** support a "delta load" strategy to efficiently update the database from one full release to the next.
    *   **R.3.3.2.2:** The delta load process **must** use temporary staging tables. The new release data will first be loaded into these staging tables.
    *   **R.3.3.2.3:** After staging, the package **must** use efficient, set-based SQL operations to integrate the data into the final target tables. This includes using `MERGE` (when available), `UPSERT` (e.g., PostgreSQL's `INSERT ... ON CONFLICT`), or a comparable `INSERT/UPDATE/DELETE` pattern to handle new, updated, and removed records.
    *   **R.3.3.2.4:** The package **must** track the currently loaded Open Targets version in a dedicated metadata table within the target database to ensure idempotency. A load for an already-processed version must be skipped.

### 3.4 Database Loading Architecture

*   **3.4.1 Abstraction Layer**
    *   **R.3.4.1.1:** The core logic **must** be built around an abstract `DatabaseLoader` base class (interface).
    *   **R.3.4.1.2:** This interface **must** define essential methods to be implemented by all concrete database backends, including:
        *   `connect()`: Establish a database connection.
        *   `prepare_staging_schema()`: Create schemas and staging tables.
        *   `bulk_load_native()`: Execute the native bulk load into a staging table.
        *   `execute_merge_strategy()`: Run the SQL to merge data from staging to final tables.
        *   `update_metadata()`: Record the outcome of the load in the metadata table.
        *   `cleanup()`: Drop staging tables and close connections.

*   **3.4.2 Extensibility**
    *   **R.3.4.2.1:** The package **must** be designed for extensibility, allowing third-party developers to add support for new database backends without modifying the core package.
    *   **R.3.4.2.2:** This plugin system **should** be implemented using Python's standard `entry_points` mechanism in `pyproject.toml`.

*   **3.4.3 PostgreSQL Implementation (Default)**
    *   **R.3.4.3.1:** The default, out-of-the-box implementation **must** be for PostgreSQL.
    *   **R.3.4.3.2:** The PostgreSQL loader **must** use the `COPY` command for maximum bulk loading performance. This should be implemented by streaming data from the Parquet reader directly to the database via a mechanism like `psycopg2.copy_expert` to avoid loading the entire dataset into memory.
    *   **R.3.4.3.3:** The loader **should** temporarily disable foreign key constraints and indexes on the target tables during the `COPY` operation and re-enable/rebuild them afterward to improve performance.

### 3.5 Execution and Configuration

*   **R.3.5.1:** The package **must** provide a Command Line Interface (CLI) for execution, allowing users to trigger and configure the loading process from a shell.
*   **R.3.5.2:** The package **must** also expose a programmatic Python API for integration into other applications and workflows.
*   **R.3.5.3:** Configuration (database credentials, source locations, load parameters) **must** be manageable via a hierarchy of sources: environment variables, and a configuration file (e.g., TOML or YAML). Sensitive information like passwords must not be passed as CLI arguments.

### 3.6 Metadata, Logging, and Monitoring

*   **R.3.6.1:** The package **must** create and manage a dedicated metadata schema (e.g., `opentargets_meta`) in the target database.
*   **R.3.6.2:** This schema **must** contain tables to track:
    *   Load history for each dataset.
    *   The Open Targets version loaded.
    *   Start and end times of each operation.
    *   Row counts (source and loaded).
    *   Success/failure status and any error messages.
*   **R.3.6.3:** The package **must** implement structured logging (e.g., in JSON format) to standard output, making it easy to parse and ingest logs into monitoring platforms.

---

## 4. Non-Functional Requirements

### 4.1 Performance

*   **NFR.4.1.1:** The system **must** be designed for high throughput, prioritizing the efficient use of network, CPU, and database resources.
*   **NFR.4.1.2:** The core loading mechanism **must** maximize the utilization of the target database's native bulk loading capabilities (e.g., PostgreSQL `COPY`). Data transfer should be streamed whenever possible to minimize memory footprint.

### 4.2 Reliability and Idempotency

*   **NFR.4.2.1:** The system **must** be resilient to transient failures (e.g., network interruptions). A failed job should be restartable without corrupting the target database.
*   **NFR.4.2.2:** All load operations **must** be idempotent. Rerunning a load for the same Open Targets version must result in the same database state as running it once successfully.

### 4.3 Security

*   **NFR.4.3.1:** The package **must** follow best practices for handling sensitive data, especially database credentials. Credentials **must not** be stored in code or configuration files in plain text. The recommended approach is retrieval from environment variables or a secure secrets management system.

---

## 5. Package Development and Maintenance

### 5.1 Packaging

*   **NFR.5.1.1:** The package **must** be defined using a `pyproject.toml` file, conforming to PEP 621.
*   **NFR.5.1.2:** A modern build backend such as Hatchling or `setuptools` **must** be used. Dependency management should be handled by a tool like Poetry or PDM.

### 5.2 Testing Strategy

*   **NFR.5.2.1:** The package **must** have a comprehensive test suite with a target code coverage of over 90%.
*   **NFR.5.2.2:** The test suite **must** use `pytest`.
*   **NFR.5.2.3:** Crucially, the suite **must** include end-to-end integration tests that spin up containerized databases (e.g., using Docker via Testcontainers) to verify the entire loading process against a real database instance.

### 5.3 CI/CD

*   **NFR.5.3.1:** A CI/CD pipeline (e.g., using GitHub Actions) **must** be established.
*   **NFR.5.3.2:** The pipeline **must** automatically run:
    *   Linting and code formatting checks (e.g., Ruff, Black).
    *   Unit and integration tests across multiple supported Python versions (3.10+).
*   **NFR.5.3.3:** The pipeline **should** automate the process of building and publishing the package to PyPI upon new version tags.

### 5.4 Documentation

*   **NFR.5.4.1:** The package **must** be accompanied by comprehensive documentation, generated using a standard tool like Sphinx or MkDocs.
*   **NFR.5.4.2:** The documentation **must** include:
    *   A "Getting Started" guide and tutorials.
    *   A detailed architectural overview.
    *   An auto-generated API reference.
    *   Instructions for configuring the loader and adding new database backends.
