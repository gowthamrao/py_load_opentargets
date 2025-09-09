# Vignette: A-to-Z guide to py-load-opentargets

This guide provides a complete walkthrough of using the `py-load-opentargets`
package, from sourcing data to loading it into your database using different
strategies.

## 1. Sourcing the Data

The first step is to configure the package to find the Open Targets data. This is
done in the `config.toml` file, under the `[source]` section. The package can
source data from Google Cloud Storage (GCS), FTP, or a local filesystem mirror.

### Default Configuration

The default configuration points to the official Open Targets GCS and FTP servers.
For most use cases, you won't need to change this.

```toml
[source]
gcs_base_url = "gs://open-targets/platform/"
ftp_host = "ftp.ebi.ac.uk"
ftp_path = "/pub/databases/opentargets/platform/"
data_uri_template = "gs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"
data_download_uri_template = "gs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"
checksum_uri_template = "gs://open-targets/platform/{version}/"
```

### Using a Local Mirror

If you have a local mirror of the Open Targets data, you can point the loader to
it by changing the `data_uri_template` and `data_download_uri_template` to use a `file://` scheme.

For example, if your data is stored in `/data/opentargets`, you would change
the configuration to:

```toml
[source]
data_uri_template = "file:///data/opentargets/{version}/output/etl/parquet/{dataset_name}"
data_download_uri_template = "file:///data/opentargets/{version}/output/etl/parquet/{dataset_name}"
```

## 2. Loading Data into a Database

Once the data source is configured, you can load the data into your database.

### Database Connection

The package requires a database connection string to be set as an environment
variable named `DB_CONN_STR`. This is to avoid storing credentials in the
configuration file.

For PostgreSQL, the connection string format is:
`postgresql://user:password@host:port/database`

You can set this in your shell like so:

```bash
export DB_CONN_STR="postgresql://user:password@host:port/database"
```

### Running the Loader

With the connection string set and the `config.toml` file in place, you can run
the loader.

To load all datasets defined in your `config.toml`:

```bash
py_load_opentargets load
```

To load only specific datasets:

```bash
py_load_opentargets load targets diseases
```

By default, the loader will find the latest version of the Open Targets data and
load it. You can specify a version with the `--version` flag:

```bash
py_load_opentargets --version 22.04 load
```

## 3. Full Refresh vs. Delta Load

The `py-load-opentargets` package offers two main strategies for loading data:
`full-refresh` and `delta`. You can choose the strategy using the `--load-type`
command-line option.

### Full Refresh

A **full refresh** completely replaces the data in the target tables. For each
dataset, the loader will:
1.  Drop the existing final table if it exists.
2.  Create a new, empty final table.
3.  Load the new data into the final table.

This strategy is useful when you want to start from a clean slate or when you
don't need to preserve any existing data in the tables.

To perform a full refresh, use the `--load-type full-refresh` flag:

```bash
py_load_opentargets --load-type full-refresh load
```

### Delta Load

A **delta load** (the default behavior) is a more sophisticated strategy that
merges the new data with the existing data in the final tables. For each dataset,
the loader will:
1.  Load the new data into a temporary staging table.
2.  Compare the staging table with the final table based on the `primary_key`
    defined in `config.toml`.
3.  **Insert** new records that are in the new data but not in the final table.
4.  **Update** existing records in the final table with the new data.
5.  **Delete** records from the final table that are not present in the new data.

This strategy is useful when you want to update your database with a new release
of the Open Targets data without losing any existing data that is still relevant.

To perform a delta load, you can either omit the `--load-type` flag (as it's the
default) or specify it explicitly:

```bash
# This is the default behavior
py_load_opentargets load

# Explicitly specifying delta load
py_load_opentargets --load-type delta load
```

## 4. Putting It All Together: A Complete Example

Let's walk through a complete example of loading the `targets` and `diseases`
datasets into a PostgreSQL database.

### 1. Configuration

First, create a `config.toml` file with the following content. This configuration
defines the two datasets and their primary keys.

```toml
# config.toml

[source]
# Using default sources

[datasets]

[datasets.targets]
primary_key = ["id"]

[datasets.diseases]
primary_key = ["id"]
```

### 2. Set Database Connection

Ensure your `DB_CONN_STR` environment variable is set:

```bash
export DB_CONN_STR="postgresql://test:test@localhost:32772/test" # Replace with your actual connection string
```

### 3. Run the Loader

Now, run the loader. We'll use the default delta load strategy.

```bash
py_load_opentargets load targets diseases
```

### 4. What Happens in the Database

After the command completes, you will have two new tables in your database:
`public.targets` and `public.diseases`.

-   The `public.targets` table will contain all the target data from the latest
    Open Targets release.
-   The `public.diseases` table will contain all the disease data.

If you run the same command again with a new release of Open Targets data, the
loader will intelligently update these tables with the new data, inserting,
updating, and deleting rows as necessary.
