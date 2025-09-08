import click
import logging
from typing import Tuple

from .config import load_config
from .data_acquisition import list_available_versions
from .backends import get_loader
from .orchestrator import ETLOrchestrator
from .logging_utils import setup_logging

# Configure logging for the CLI
logger = logging.getLogger(__name__)


@click.group()
@click.option('--config', 'config_path', type=click.Path(exists=True), help='Path to a custom config.toml file.')
@click.pass_context
def cli(ctx, config_path):
    """A CLI tool to download and load Open Targets data."""
    setup_logging()
    ctx.ensure_object(dict)
    # Load config and attach it to the click context
    ctx.obj['CONFIG'] = load_config(config_path)


@cli.command(name="list-versions")
@click.pass_context
def list_versions_cmd(ctx):
    """Lists the available Open Targets release versions."""
    config = ctx.obj['CONFIG']
    source_config = config['source']
    click.echo("Discovering available versions...")
    try:
        versions = list_available_versions(source_config['version_discovery_uri'])
        if versions:
            click.echo(click.style("Available versions (newest first):", bold=True))
            for version in versions:
                click.echo(f"- {version}")
        else:
            click.secho("Could not find any available Open Targets versions.", fg='yellow')
    except Exception as e:
        click.secho(f"Error during version discovery: {e}", fg='red')
        raise click.Abort()


@cli.command()
@click.option('--db-conn-str', required=True, envvar='DB_CONN_STR', help='Database connection string.')
@click.option('--backend', default='postgres', show_default=True, help='Database backend to use (e.g., postgres).')
@click.option('--version', help='Open Targets version to load. Defaults to the latest.')
@click.option('--staging-schema', default='staging', show_default=True, help='Schema for staging tables.')
@click.option('--final-schema', default='public', show_default=True, help='Schema for the final table.')
@click.option('--skip-confirmation', is_flag=True, help='Skip confirmation prompt for loading already loaded versions.')
@click.option('--no-continue-on-error', is_flag=True, help='Abort the process if an error occurs for any dataset.')
@click.argument('datasets', nargs=-1)
@click.pass_context
def load(ctx, db_conn_str, backend, version, staging_schema, final_schema, skip_confirmation, no_continue_on_error, datasets: Tuple[str]):
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
    datasets_to_process = list(datasets or all_defined_datasets.keys())
    click.echo(f"--- Open Targets Universal Loader ---")
    click.echo(f"Selected datasets: {click.style(', '.join(datasets_to_process), bold=True)}")

    # 2. Determine version
    if not version:
        click.echo("Discovering the latest version...")
        try:
            versions = list_available_versions(source_config['version_discovery_uri'])
            if not versions:
                click.secho("Error: Could not find any Open Targets versions.", fg='red')
                raise click.Abort()
            version = versions[0]
            click.echo(f"Found latest version: {version}")
        except Exception as e:
            click.secho(f"Error during version discovery: {e}", fg='red')
            raise click.Abort()

    # 3. Get database loader
    try:
        loader = get_loader(backend)
    except (ValueError, ImportError) as e:
        click.secho(f"Error: {e}", fg='red')
        raise click.Abort()

    # 4. Connect to DB and run the orchestrator
    try:
        loader.connect(db_conn_str)

        # The confirmation for re-processing is now handled inside the CLI
        # before handing off to the non-interactive orchestrator.
        if not skip_confirmation:
            for dataset_name in datasets_to_process:
                last_successful_version = loader.get_last_successful_version(dataset_name)
                if last_successful_version == version:
                    if not click.confirm(f"⚠️ Version '{version}' of '{dataset_name}' already loaded. Continue?"):
                        click.echo(f"Skipping dataset '{dataset_name}' as requested.")
                        datasets_to_process.remove(dataset_name)

        if not datasets_to_process:
            click.echo("No datasets left to process. Exiting.")
            return

        orchestrator = ETLOrchestrator(
            config=config,
            loader=loader,
            datasets_to_process=datasets_to_process,
            version=version,
            staging_schema=staging_schema,
            final_schema=final_schema,
            # The orchestrator is always non-interactive.
            skip_confirmation=True,
            continue_on_error=not no_continue_on_error
        )
        orchestrator.run()

    except Exception as e:
        # Catch fatal errors (DB connection, or re-raised from orchestrator)
        click.secho(f"A fatal error occurred: {e}", fg='red')
        raise click.Abort()
    finally:
        # Final cleanup
        if 'loader' in locals() and loader.conn:
            loader.cleanup()
        click.echo("\n--- CLI Process Finished ---")


if __name__ == '__main__':
    cli()
