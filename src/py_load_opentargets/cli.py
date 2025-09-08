import click
import logging
from typing import Tuple

from .config import load_config
from .data_acquisition import list_available_versions
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
    # This also populates the new 'database' section with defaults if not present
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
@click.option('--version', help='Open Targets version to load. Defaults to the latest.')
@click.option('--staging-schema', default='staging', show_default=True, help='Schema for staging tables.')
@click.option('--final-schema', default='public', show_default=True, help='Schema for the final table.')
@click.option('--skip-confirmation', is_flag=True, help='Skip confirmation prompt for loading already loaded versions.')
@click.option('--no-continue-on-error', is_flag=True, help='Abort the process if an error occurs for any dataset.')
@click.argument('datasets', nargs=-1)
@click.pass_context
def load(ctx, version, staging_schema, final_schema, skip_confirmation, no_continue_on_error, datasets: Tuple[str]):
    """
    Downloads and loads specified Open Targets datasets into a database.

    This command orchestrates the full ETL process for each specified dataset.
    If no datasets are provided, it will process ALL datasets defined in the
    configuration file.
    """
    config = ctx.obj['CONFIG']
    source_config = config['source']
    all_defined_datasets = config['datasets']

    datasets_to_process = list(datasets or all_defined_datasets.keys())
    click.echo(f"--- Open Targets Universal Loader ---")
    click.echo(f"Selected datasets: {click.style(', '.join(datasets_to_process), bold=True)}")

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

    try:
        orchestrator = ETLOrchestrator(
            config=config,
            datasets_to_process=datasets_to_process,
            version=version,
            staging_schema=staging_schema,
            final_schema=final_schema,
            skip_confirmation=skip_confirmation,
            continue_on_error=not no_continue_on_error
        )
        orchestrator.run()

    except Exception as e:
        # Catch fatal errors (DB connection string not set, or re-raised from orchestrator)
        click.secho(f"A fatal error occurred: {e}", fg='red')
        raise click.Abort()
    finally:
        click.echo("\n--- CLI Process Finished ---")


if __name__ == '__main__':
    cli()
