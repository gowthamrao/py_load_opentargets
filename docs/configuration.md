# Configuration Reference

The `py_load_opentargets` tool is configured using a single `config.toml` file. This page provides a reference for all available settings.

## Top-Level Sections

The configuration is organized into the following main sections:
- `[source]`: Defines where to find the Open Targets data.
- `[database]`: Configures settings related to the target database.
- `[execution]`: Controls the execution behavior of the loader.
- `[datasets]`: Defines the specific datasets to be processed.

---

## `[source]`

This section tells the loader where to find the Open Targets data releases.

- **`gcs_base_url`** (string, optional)
  - The base URL for Google Cloud Storage. Defaults to the official Open Targets bucket.
  - *Default*: `"gs://open-targets/platform/"`

- **`ftp_host`** (string, optional)
  - The hostname for the FTP server. Defaults to the EBI FTP server.
  - *Default*: `"ftp.ebi.ac.uk"`

- **`data_uri_template`** (string, required)
  - An `fsspec`-compatible URI template for locating the Parquet files for each dataset. This is used when the `load_strategy` is set to `"stream"`.
  - It must contain `{version}` and `{dataset_name}` placeholders.
  - *Example*: `"gs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"`

- **`data_download_uri_template`** (string, required)
  - Similar to `data_uri_template`, but used when the `load_strategy` is `"download"`.
  - *Example*: `"gs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"`

- **`checksum_uri_template`** (string, required)
  - A template for the directory containing the `release_data_integrity` checksum file.
  - It must contain a `{version}` placeholder.
  - *Example*: `"gs://open-targets/platform/{version}/"`

---

## `[database]`

This section controls database-specific behavior.

- **`backend`** (string, optional)
  - The name of the database backend to use. This must match a registered `entry_point`.
  - *Default*: `"postgres"`

- **`staging_schema`** (string, optional)
  - The name of the database schema where temporary staging tables will be created.
  - *Default*: `"staging"`

- **`final_schema`** (string, optional)
  - The name of the database schema where the final, permanent tables will be stored.
  - *Default*: `"public"`

- **`flatten_separator`** (string, optional)
  - The character used to separate flattened struct field names.
  - *Default*: `"_"`
  - *Example*: A struct `drug` with a field `id` becomes `drug_id`.

---

## `[execution]`

This section controls how the ETL process runs.

- **`max_workers`** (integer, optional)
  - The number of datasets to process in parallel.
  - *Default*: `1` (sequential processing)

- **`load_strategy`** (string, optional)
  - Determines how data is handled.
  - **`"download"`**: (Default) Files are downloaded to a temporary local directory before being loaded into the database. This is robust but requires local disk space.
  - **`"stream"`**: Files are streamed directly from the remote source into the database without being saved to disk. This is faster and requires no disk space, but may be less resilient to network interruptions.
  - *Default*: `"download"`

- **`continue_on_error`** (boolean, optional)
  - If `true`, the failure of one dataset will not stop the processing of other datasets.
  - If `false`, the entire process will halt as soon as any single dataset fails.
  - *Default*: `true`

---

## `[datasets]`

This is the most important section, where you define each dataset you want to load. Each dataset gets its own sub-table, e.g., `[datasets.targets]`.

For each dataset, the following keys are available:

- **`primary_key`** (list of strings, required)
  - A list of column names that uniquely identify a row. This is critical for the `UPSERT` and `DELETE` logic in the merge strategy.

- **`final_table_name`** (string, optional)
  - The name of the final table in the database.
  - If not provided, it defaults to the dataset name (e.g., `associationByOverallDirect` becomes `associationbyoveralldirect`). It's often a good idea to specify a cleaner name.

- **`flatten_structs`** (list of strings, optional)
  - A list of column names of type `struct` that you want to flatten into top-level columns.
  - Any `struct` column *not* listed here will be serialized and loaded into a `JSONB` column.
  - All `list` columns are always loaded as `JSONB`.

### Example Dataset Configuration

```toml
[datasets.molecule]
primary_key = ["id"]
final_table_name = "drugs"
# The 'drug' column is a struct. We want to flatten it.
flatten_structs = ["drug"]

[datasets.evidence]
primary_key = ["id"]
# Here, we don't specify flatten_structs, so any nested columns
# will be loaded as JSONB, which might be desirable for complex,
# deeply nested data like in the evidence dataset.
```
