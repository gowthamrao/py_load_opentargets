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
@click.option('--db-conn-str', required=True, envvar='DB_CONN_STR', help='Database connection string.')
@click.option('--version', help='Open Targets version to load. Defaults to the latest.')
@click.option('--dataset', required=True, help="Name of the dataset (e.g., 'targets', 'diseases').")
@click.option('--primary-key', required=True, multiple=True, help='Primary key column(s) for the merge. Can be specified multiple times.')
@click.option('--staging-schema', default='staging', show_default=True, help='Schema for staging tables.')
@click.option('--final-schema', default='public', show_default=True, help='Schema for the final table.')
@click.option('--final-table-name', help='Name of the final table. Defaults to the dataset name.')
@click.option('--skip-confirmation', is_flag=True, help='Skip confirmation prompt for loading already loaded versions.')
def load_postgres(db_conn_str, version, dataset, primary_key, staging_schema, final_schema, final_table_name, skip_confirmation):
    """
    Downloads, loads, and merges a specific Open Targets dataset into PostgreSQL.

    This command orchestrates the full ETL process for a single dataset:
    1. Finds the latest Open Targets version if not specified.
    2. Downloads the dataset to a temporary directory.
    3. Loads the data into a staging table.
    4. Merges the data from the staging table into the final table using an UPSERT strategy.
    5. Records the outcome in a metadata table.
    """
    click.echo("--- Open Targets PostgreSQL Loader ---")
    final_table = final_table_name or dataset.replace('-', '_')
    final_table_full_name = f"{final_schema}.{final_table}"
    dataset_name_sanitized = dataset.replace('-', '_')
    staging_table_name = f"{staging_schema}.{dataset_name_sanitized}"

    # 1. Determine version
    if not version:
        click.echo("Discovering the latest version...")
        try:
            versions = list_available_versions()
            if not versions:
                click.secho("Error: Could not find any Open Targets versions.", fg='red'); raise click.Abort()
            version = versions[0]
            click.echo(f"Found latest version: {version}")
        except Exception as e:
            click.secho(f"Error during version discovery: {e}", fg='red'); raise click.Abort()

    click.echo(f"Target Version: {click.style(version, bold=True)}")
    click.echo(f"Target Dataset: {click.style(dataset, bold=True)}")
    click.echo(f"Primary Key(s): {click.style(str(primary_key), bold=True)}")
    click.echo(f"Staging Table: {click.style(staging_table_name, bold=True)}")
    click.echo(f"Final Table: {click.style(final_table_full_name, bold=True)}")

    loader = PostgresLoader()
    row_count = 0
    try:
        loader.connect(db_conn_str)

        # Idempotency Check
        last_successful_version = loader.get_last_successful_version(dataset)
        if last_successful_version == version and not skip_confirmation:
            click.confirm(
                f"⚠️ Version '{version}' of dataset '{dataset}' has already been successfully loaded. Continue anyway?",
                abort=True
            )

        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)
            # 2. Download
            click.echo(f"\nDownloading data to {temp_dir}...")
            try:
                parquet_path = download_dataset(version, dataset, temp_dir)
            except Exception as e:
                click.secho(f"Fatal: Download failed. {e}", fg='red'); raise click.Abort()

            # 3. Stage
            click.echo("Preparing staging schema and table...")
            loader.prepare_staging_schema(staging_schema)
            loader.prepare_staging_table(staging_table_name, parquet_path)

            click.echo(f"Bulk loading data into staging table '{staging_table_name}'...")
            row_count = loader.bulk_load_native(staging_table_name, parquet_path)
            click.echo(f"Loaded {row_count} rows into staging.")

            # 4. Merge
            click.echo(f"Merging data into final table '{final_table_full_name}'...")
            loader.execute_merge_strategy(staging_table_name, final_table_full_name, list(primary_key))

            # 5. Success Metadata
            loader.update_metadata(version=version, dataset=dataset, success=True, row_count=row_count)
            click.secho(f"\n✅ Success! Merged {row_count} rows into '{final_table_full_name}'.", fg='green')

    except Exception as e:
        # Failure Metadata
        click.secho(f"\n❌ Error: An error occurred during the process: {e}", fg='red')
        if loader.conn:
            error_message = str(e).replace('\n', ' ').strip()
            loader.update_metadata(version=version, dataset=dataset, success=False, row_count=row_count, error_message=error_message)
        raise click.Abort()
    finally:
        if loader.conn:
            # Cleanup: drop staging table on success
            if 'e' not in locals(): # Check if an exception occurred
                try:
                    click.echo(f"Dropping staging table '{staging_table_name}'...")
                    loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                    loader.conn.commit()
                except Exception as cleanup_e:
                    click.secho(f"Warning: Failed to drop staging table: {cleanup_e}", fg='yellow')
            loader.cleanup()

    click.echo("\n--- Process Complete ---")

if __name__ == '__main__':
    cli()
