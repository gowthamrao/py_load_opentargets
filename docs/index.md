# py_load_opentargets

A package to efficiently load Open Targets data into a relational database, designed for performance, extensibility, and ease of use.

This tool allows you to download and load the entire Open Targets platform dataset (or a subset of it) into a database like PostgreSQL with a single command. It handles schema inference, data merging (UPSERT), and metadata tracking automatically.

## Key Features

- **Configuration-Driven**: Control the entire loading process with a simple `config.toml` file.
- **Extensible**: Supports multiple database backends through a pluggable system (currently supports PostgreSQL).
- **High Performance**: Uses native database bulk loading (`COPY` for PostgreSQL) for maximum speed.
- **Idempotent**: Safely re-run loads for the same version without creating duplicate data.
- **Schema-Aware**: Automatically creates tables, infers schemas, and handles schema evolution between releases.
- **Automated**: Downloads the latest (or a specific) version of Open Targets data.

## Quickstart

### 1. Installation

```bash
pip install .
```

### 2. Configuration

Create a `config.toml` file to customize the behavior of the loader. You can start by copying the [default configuration](src/py_load_opentargets/default_config.toml).

A minimal `config.toml` might look like this:

```toml
# You can override the default GCS/FTP sources if you have a local mirror
[source]
gcs_base_url = "gs://open-targets/platform/"
ftp_host = "ftp.ebi.ac.uk"
ftp_path = "/pub/databases/opentargets/platform/"

# Define the datasets you care about and their primary keys for merging.
[datasets]

[datasets.targets]
primary_key = ["id"]

[datasets.diseases]
primary_key = ["id"]

[datasets.associationByOverallDirect]
primary_key = ["targetId", "diseaseId"]
final_table_name = "association_direct_overall"
```

### 3. Set Database Credentials

The loader requires a database connection string. It's recommended to set this as an environment variable:

```bash
export DB_CONN_STR="postgresql://user:password@host:port/database"
```

### 4. Run the Loader

**To load ALL datasets defined in your configuration:**

```bash
py_load_opentargets load
```

**To load only specific datasets:**

```bash
py_load_opentargets load targets diseases
```

The tool will:
1. Find the latest Open Targets version.
2. Download each dataset.
3. Load data into a staging table.
4. Merge data into the final table using the defined primary keys.
5. Record the process in a `_ot_load_metadata` table.

## Contributing

Contributions are welcome! Please see the `CONTRIBUTING.md` file for more details.
