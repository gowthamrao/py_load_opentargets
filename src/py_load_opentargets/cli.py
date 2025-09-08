import click
import tempfile
import logging
from pathlib import Path
from typing import Tuple

from .config import load_config, get_config
from .data_acquisition import list_available_versions, download_dataset
from .backends import get_loader

# Configure logging for the CLI
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@click.group()
@click.option('--config', 'config_path', type=click.Path(exists=True), help='Path to a custom config.toml file.')
@click.pass_context
def cli(ctx, config_path):
    """A CLI tool to download and load Open Targets data."""
    ctx.ensure_object(dict)
    # Load config and attach it to the click context
    ctx.obj['CONFIG'] = load_config(config_path)

@cli.command()
@click.option('--db-conn-str', required=True, envvar='DB_CONN_STR', help='Database connection string.')
@click.option('--backend', default='postgres', show_default=True, help='Database backend to use (e.g., postgres).')
@click.option('--version', help='Open Targets version to load. Defaults to the latest.')
@click.option('--staging-schema', default='staging', show_default=True, help='Schema for staging tables.')
@click.option('--final-schema', default='public', show_default=True, help='Schema for the final table.')
@click.option('--skip-confirmation', is_flag=True, help='Skip confirmation prompt for loading already loaded versions.')
@click.argument('datasets', nargs=-1)
@click.pass_context
def load(ctx, db_conn_str, backend, version, staging_schema, final_schema, skip_confirmation, datasets: Tuple[str]):
    """
    Downloads and loads specified Open Targets datasets into a database.

    This command orchestrates the full ETL process for each specified dataset.
    If no datasets are provided, it will process ALL datasets defined in the
    configuration file.
    """
    config = ctx.obj['CONFIG']
    source_config = config['source']
    all_defined_datasets = config['datasets']

    # 1. Determine which datasets to process
    datasets_to_process = datasets or all_defined_datasets.keys()
    click.echo(f"--- Open Targets Universal Loader ---")
    click.echo(f"Selected datasets: {click.style(', '.join(datasets_to_process), bold=True)}")

    # 2. Determine version
    if not version:
        click.echo("Discovering the latest version...")
        try:
            versions = list_available_versions(source_config['ftp_host'], source_config['ftp_path'])
            if not versions:
                click.secho("Error: Could not find any Open Targets versions.", fg='red'); raise click.Abort()
            version = versions[0]
            click.echo(f"Found latest version: {version}")
        except Exception as e:
            click.secho(f"Error during version discovery: {e}", fg='red'); raise click.Abort()

    # 3. Get database loader
    try:
        loader = get_loader(backend)
    except (ValueError, ImportError) as e:
        click.secho(f"Error: {e}", fg='red'); raise click.Abort()

    # 4. Connect to DB
    try:
        loader.connect(db_conn_str)
    except Exception as e:
        click.secho(f"Failed to connect to database: {e}", fg='red'); raise click.Abort()

    # 5. Process each dataset in a loop
    for dataset_name in datasets_to_process:
        if dataset_name not in all_defined_datasets:
            click.secho(f"Warning: Dataset '{dataset_name}' is not defined in the configuration. Skipping.", fg='yellow')
            continue

        dataset_config = all_defined_datasets[dataset_name]
        primary_keys = dataset_config['primary_key']
        final_table = dataset_config.get('final_table_name', dataset_name.replace('-', '_'))
        final_table_full_name = f"{final_schema}.{final_table}"
        staging_table_name = f"{staging_schema}.{dataset_name.replace('-', '_')}"

        click.echo("\n" + "="*80)
        click.echo(f"Processing dataset: {click.style(dataset_name, fg='cyan', bold=True)}")
        click.echo(f"  - Version: {version}")
        click.echo(f"  - Primary Keys: {primary_keys}")
        click.echo(f"  - Staging Table: {staging_table_name}")
        click.echo(f"  - Final Table: {final_table_full_name}")
        click.echo("="*80)

        row_count = 0
        try:
            # Idempotency Check
            last_successful_version = loader.get_last_successful_version(dataset_name)
            if last_successful_version == version and not skip_confirmation:
                if not click.confirm(f"⚠️ Version '{version}' of '{dataset_name}' already loaded. Continue?"):
                    click.echo("Skipping.")
                    continue

            with tempfile.TemporaryDirectory() as temp_dir_str:
                temp_dir = Path(temp_dir_str)
                click.echo(f"Downloading data to {temp_dir}...")
                parquet_path = download_dataset(source_config['gcs_base_url'], version, dataset_name, temp_dir)

                click.echo("Preparing staging schema and table...")
                loader.prepare_staging_schema(staging_schema)
                loader.prepare_staging_table(staging_table_name, parquet_path)

                click.echo(f"Bulk loading into staging table '{staging_table_name}'...")
                row_count = loader.bulk_load_native(staging_table_name, parquet_path)
                click.echo(f"Loaded {row_count} rows into staging.")

                if loader._table_exists(final_table_full_name):
                    click.echo("Aligning schema of final table...")
                    loader.align_final_table_schema(staging_table_name, final_table_full_name)

                click.echo(f"Merging data into final table '{final_table_full_name}'...")
                loader.execute_merge_strategy(staging_table_name, final_table_full_name, primary_keys)

                loader.update_metadata(version=version, dataset=dataset_name, success=True, row_count=row_count)
                click.secho(f"✅ Success! Merged {row_count} rows for dataset '{dataset_name}'.", fg='green')

        except Exception as e:
            click.secho(f"\n❌ Error processing dataset '{dataset_name}': {e}", fg='red')
            error_message = str(e).replace('\n', ' ').strip()
            loader.update_metadata(version=version, dataset=dataset_name, success=False, row_count=row_count, error_message=error_message)
            # Decide if we should abort all or continue with next dataset
            if not click.confirm("An error occurred. Continue with the next dataset?"):
                loader.cleanup()
                raise click.Abort()
        finally:
            # Cleanup staging table for the processed dataset
            try:
                logger.info(f"Dropping staging table '{staging_table_name}'...")
                loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table_name};")
                loader.conn.commit()
            except Exception as cleanup_e:
                click.secho(f"Warning: Failed to drop staging table '{staging_table_name}': {cleanup_e}", fg='yellow')

    # Final cleanup
    loader.cleanup()
    click.echo("\n--- Full Process Complete ---")

if __name__ == '__main__':
    cli()
