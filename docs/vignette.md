# Vignette: A-to-Z Guide to `py-load-opentargets`

This guide provides a complete walkthrough of using the `py-load-opentargets` package, from sourcing data to loading it into your database using different strategies.

## 1. Sourcing the Data

The first step is to configure how the package finds and downloads Open Targets data. This is controlled by the `[source]` section in your `config.toml` file. The package supports the default public Open Targets sources (GCS and FTP) as well as private mirrors on S3 or a local filesystem.

### Default Configuration (Recommended)

For most users, the default configuration is sufficient. The package is pre-configured to use the official Open Targets GCS and FTP servers to find and download data. You don't need to add anything to your `config.toml` for this to work.

Here is what the default source configuration looks like internally:
```toml
# In default_config.toml
[source]
provider = "gcs_ftp"

[source.gcs_ftp]
version_discovery_uri = "ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform/"
dataset_discovery_uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/"
data_download_uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}/"
```

### Using a Private S3 Mirror

If you maintain a private mirror of the Open Targets data in an S3 bucket, you can configure the loader to use it.

First, ensure you have the necessary extra dependency installed:
```bash
pip install .[s3]
```

Then, update your `config.toml` to set the provider to `"s3"` and specify your bucket details:

```toml
# In your config.toml
[source]
provider = "s3"

[source.s3]
version_discovery_uri = "s3://your-opentargets-bucket/platform/"
dataset_discovery_uri_template = "s3://your-opentargets-bucket/platform/{version}/output/etl/parquet/"
data_download_uri_template = "s3://your-opentargets-bucket/platform/{version}/output/etl/parquet/{dataset_name}/"
```
*You must have your environment configured with AWS credentials for this to work (e.g., via `~/.aws/credentials` or environment variables like `AWS_ACCESS_KEY_ID`).*

## 2. Specifying the Target Database

The loader needs to know where to load the data. This is configured via a database connection string.

### Database Connection

To avoid storing sensitive credentials in configuration files, the package requires the database connection string to be set as an environment variable named `DB_CONN_STR`.

For PostgreSQL (the default backend), the connection string format is:
`postgresql://user:password@host:port/database`

You can set this in your shell like so:
```bash
export DB_CONN_STR="postgresql://myuser:mypassword@localhost:5432/opentargets"
```

The tool also includes a validation step to check this connection before starting a load. You can run it manually:
```bash
py_load_opentargets validate
```

## 3. Configuring Datasets

Before you can load data, you must define which datasets you are interested in and tell the loader how to handle them. This is done in the `[datasets]` section of your `config.toml`.

For each dataset you want to load, you must specify a `primary_key`. This is crucial for the default `delta` load to identify unique records for updates and insertions.

### Basic Dataset Configuration

Here’s an example of a minimal configuration for loading the `targets` and `diseases` datasets:
```toml
# In config.toml
[datasets]

[datasets.targets]
primary_key = ["id"]

[datasets.diseases]
primary_key = ["id"]
```

### Discovering Available Datasets

If you're unsure which datasets are available for a specific Open Targets release, you can use the `discover-datasets` command:
```bash
# Discover datasets for the latest version
py_load_opentargets discover-datasets --version 22.04 --format toml
```
This will output a TOML snippet that you can copy directly into your `config.toml`.

## 4. Running the Loader: Full vs. Incremental Loads

The `py-load-opentargets` package offers two main strategies for loading data: `full-refresh` and `delta` (incremental). You can choose the strategy using the `--load-type` command-line option.

### Full Refresh

A **full refresh** is a destructive operation that completely replaces the data in the target tables. For each dataset, the loader will:
1.  Drop the existing final table if it exists.
2.  Create a new, empty final table based on the data's schema.
3.  Load all the new data into the final table.

This strategy is useful when you want to start from a clean slate or when the schema has changed significantly.

To perform a full refresh for all datasets in your config:
```bash
py_load_opentargets --load-type full-refresh load
```
To run it for specific datasets:
```bash
py_load_opentargets --load-type full-refresh load targets diseases
```

### Incremental (Delta) Load

An **incremental load** (the default behavior, called `delta`) is a more sophisticated strategy that merges new data with existing data. For each dataset, the loader will:
1.  Load the new data into a temporary staging table.
2.  Compare the staging table with the final table using the `primary_key` you defined.
3.  **Insert** new records.
4.  **Update** existing records.
5.  **Delete** records from the final table that are no longer present in the new data source.

This is the recommended approach for updating your database with a new release of Open Targets data, as it preserves the table history and is generally faster.

To perform a delta load, you can either omit the `--load-type` flag or specify it explicitly:
```bash
# This is the default behavior
py_load_opentargets load

# Explicitly specifying delta load
py_load_opentargets --load-type delta load targets diseases
```

## 5. Putting It All Together: A Complete Example

Let's walk through a complete example of loading the `targets` and `diseases` datasets into a PostgreSQL database using an incremental `delta` load.

### Step 1: Create `config.toml`

Create a `config.toml` file with the following content.
```toml
# config.toml

# We will use the default source, so no [source] section is needed.

# Define the datasets we want to load and their primary keys.
[datasets]

[datasets.targets]
primary_key = ["id"]

[datasets.diseases]
primary_key = ["id"]

[datasets.associationByOverallDirect]
primary_key = ["targetId", "diseaseId"]
final_table_name = "association_direct_overall"
```

### Step 2: Set Database Connection

In your terminal, set the `DB_CONN_STR` environment variable:
```bash
export DB_CONN_STR="postgresql://test:test@localhost:5432/test_db" # Replace with your actual connection string
```

### Step 3: Run the Loader

Now, run the loader. We'll load only the `targets` and `diseases` datasets. Since we don't specify `--load-type`, it will default to `delta`.
```bash
# The tool will automatically find the latest Open Targets version
py_load_opentargets load targets diseases
```

You can also specify a particular version:
```bash
py_load_opentargets --version 22.04 load targets diseases
```

### Step 4: Check the Database

After the command completes, you will have two new tables in your database: `public.targets` and `public.diseases`. If you run the command again with a newer Open Targets release, these tables will be updated with the new data.

This concludes the A-to-Z guide. You are now equipped to use `py-load-opentargets` to build and maintain your own Open Targets database.
