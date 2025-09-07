import click
import tempfile
import logging
from pathlib import Path

from .data_acquisition import list_available_versions, download_dataset
from .backends.postgres import PostgresLoader

# Configure logging for the CLI
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@click.group()
def cli():
    """A CLI tool to download and load Open Targets data."""
    pass

@cli.command()
@click.option('--db-conn-str', required=True, envvar='DB_CONN_STR', help='Database connection string (can also be set via DB_CONN_STR env var).')
@click.option('--version', help='Open Targets version to load. If not provided, the latest version will be used.')
@click.option('--dataset', required=True, help="The name of the dataset to load (e.g., 'targets', 'diseases').")
@click.option('--staging-schema', default='staging', show_default=True, help='The database schema for staging tables.')
def load_postgres(db_conn_str, version, dataset, staging_schema):
    """
    Downloads and loads a specific Open Targets dataset into PostgreSQL.

    This command orchestrates the entire ETL process for a single dataset:
    1. Discovers the latest Open Targets version if one is not specified.
    2. Downloads the specified dataset (as partitioned Parquet files) to a temporary local directory.
    3. Connects to the PostgreSQL database.
    4. Creates a staging table with a schema inferred from the Parquet files.
    5. Uses the high-performance COPY command to bulk load the data into the staging table.
    """
    click.echo("--- Open Targets PostgreSQL Loader ---")

    # 1. Determine the target version
    if not version:
        click.echo("No version specified, discovering the latest...")
        try:
            available_versions = list_available_versions()
            if not available_versions:
                click.secho("Error: Could not discover any available Open Targets versions. Exiting.", fg='red')
                raise click.Abort()
            version = available_versions[0]
            click.echo(f"Found latest version: {version}")
        except Exception as e:
            click.secho(f"An error occurred during version discovery: {e}", fg='red')
            raise click.Abort()

    click.echo(f"Target Version: {click.style(version, bold=True)}")
    click.echo(f"Target Dataset: {click.style(dataset, bold=True)}")

    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)

        # 2. Download the data
        click.echo(f"Downloading data to temporary directory: {temp_dir}...")
        try:
            parquet_path = download_dataset(version, dataset, temp_dir)
        except Exception as e:
            click.secho(f"Fatal: Failed to download dataset '{dataset}' for version '{version}'. Error: {e}", fg='red')
            raise click.Abort()

        # 3. Initialize loader and connect to the database
        loader = PostgresLoader()
        try:
            loader.connect(db_conn_str)

            # 4. Prepare schema and table
            loader.prepare_staging_schema(staging_schema)
            staging_table_name = f"{staging_schema}.{dataset.replace('-', '_')}" # Sanitize dataset name

            loader.prepare_staging_table(staging_table_name, parquet_path)

            # 5. Bulk load data
            row_count = loader.bulk_load_native(staging_table_name, parquet_path)

            click.secho(f"\n✅ Success! Loaded {row_count} rows into '{staging_table_name}'.", fg='green')

        except Exception as e:
            click.secho(f"\n❌ Error: An error occurred during the database loading process: {e}", fg='red')
            raise click.Abort()
        finally:
            if loader.conn:
                loader.cleanup()

    click.echo("\n--- Process Complete ---")

if __name__ == '__main__':
    cli()
