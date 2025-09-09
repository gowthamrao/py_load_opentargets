# Getting Started

This guide provides a detailed walkthrough for setting up and running `py-load-opentargets` for the first time.

## 1. Installation

The package is distributed via PyPI. You can install it using `pip`. It is highly recommended to install it in a virtual environment.

```bash
python -m venv .venv
source .venv/bin/activate
pip install py-load-opentargets
```

If you have a local clone of the repository, you can install it in editable mode:
```bash
pip install -e .
```

## 2. Configuration

The entire ETL process is controlled by a configuration file, typically named `config.toml`. You can create one in your project directory.

A minimal configuration looks like this:

```toml
# config.toml

# This section defines where to find Open Targets data.
# The defaults usually work, but you can point to a local mirror.
[source]
gcs_base_url = "gs://open-targets/platform/"
ftp_host = "ftp.ebi.ac.uk"
ftp_path = "/pub/databases/opentargets/platform/"
data_uri_template = "gs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"
data_download_uri_template = "gs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"
checksum_uri_template = "gs://open-targets/platform/{version}/"

# This section defines which datasets to process and how to handle them.
[datasets]

# Example for the 'targets' dataset
[datasets.targets]
# The primary key is crucial for the merge (UPSERT) operation.
primary_key = ["id"]

# Example for the 'diseases' dataset
[datasets.diseases]
primary_key = ["id"]

# You can customize the final table name.
# If not provided, it defaults to the dataset name.
[datasets.associationByOverallDirect]
primary_key = ["targetId", "diseaseId"]
final_table_name = "association_direct_overall"
```

## 3. Database Credentials

The loader needs to connect to your PostgreSQL database. The connection string should be provided via the `DB_CONN_STR` environment variable. This practice avoids storing sensitive credentials in configuration files.

```bash
export DB_CONN_STR="postgresql://your_user:your_password@your_host:5432/your_database"
```

## 4. Running the Loader

Once configured, you can run the loader from your terminal.

### Choosing a Version
By default, the tool will automatically discover and use the latest Open Targets release. You can also specify a version with the `--version` flag:
```bash
py-load-opentargets --version 24.06 load
```

### Loading Data
To load all datasets defined in your `config.toml`:
```bash
py-load-opentargets load
```

To load only a specific subset of datasets:
```bash
py-load-opentargets load targets diseases
```

The tool will perform the following steps for each dataset:
1.  **Verify Data Integrity**: Download a checksum manifest and verify all remote files before processing.
2.  **Create Staging Table**: Create a temporary staging table in the database.
3.  **Bulk Load**: Stream the data from the source Parquet files directly into the staging table using PostgreSQL's `COPY` command.
4.  **Merge Data**:
    - Add any new columns to the final table if the schema has evolved.
    - Delete any records from the final table that are no longer in the new data release.
    - Insert new records and update existing records (UPSERT).
5.  **Track Metadata**: Record the outcome of the load (version, row count, duration, status) in the `_ot_load_metadata` table.
