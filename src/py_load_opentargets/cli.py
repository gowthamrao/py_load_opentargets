import click
import logging
from typing import Tuple

from .config import load_config
from .data_acquisition import list_available_versions
from .orchestrator import ETLOrchestrator
from .logging_utils import setup_logging
from .validator import ValidationService

# Configure logging for the CLI
logger = logging.getLogger(__name__)


from .logging_utils import setup_logging

@click.group()
@click.option('--config', 'config_path', type=click.Path(exists=True), help='Path to a custom config.toml file.')
@click.option('--json-logs', is_flag=True, help='Output logs in JSON format. Overrides config file setting.')
@click.pass_context
def cli(ctx, config_path, json_logs):
    """A CLI tool to download and load Open Targets data."""
    ctx.ensure_object(dict)
    config = load_config(config_path)
    ctx.obj['CONFIG'] = config

    # Setup logging
    # The CLI flag --json-logs takes precedence over the config file setting.
    use_json_logging = json_logs or config.get('logging', {}).get('json_format', False)
    setup_logging(json_format=use_json_logging)


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


@cli.command(name="discover-datasets")
@click.option('--version', required=True, help='The Open Targets version to inspect.')
@click.option('--format', type=click.Choice(['list', 'toml'], case_sensitive=False), default='list', show_default=True, help='Output format.')
@click.pass_context
def discover_datasets_cmd(ctx, version, format):
    """Discovers the available datasets for a given Open Targets version."""
    config = ctx.obj['CONFIG']
    source_config = config['source']

    # Prefer a dedicated URI template for discovering datasets.
    uri_template = source_config.get('dataset_discovery_uri_template')
    datasets_uri = ""
    if uri_template:
        datasets_uri = uri_template.format(version=version)
    else:
        # Fallback to deriving from the data_uri_template
        logger.warning("`dataset_discovery_uri_template` not found in config, attempting to derive from `data_uri_template`.")
        data_uri_template = source_config.get('data_uri_template', '')
        if '{dataset_name}' not in data_uri_template:
            click.secho("Error: 'data_uri_template' in config is missing the {{dataset_name}} placeholder.", fg='red')
            raise click.Abort()
        # Get the base path before the dataset name placeholder
        datasets_uri = data_uri_template.split('{dataset_name}')[0].format(version=version)

    click.echo(f"Discovering datasets for version {click.style(version, bold=True)} at {datasets_uri}...")

    # Import lazily to avoid circular dependencies if ever needed
    from .data_acquisition import discover_datasets
    datasets = discover_datasets(datasets_uri)

    if not datasets:
        click.secho("Could not find any datasets for this version. Check the version number and your source configuration.", fg='yellow')
        return

    click.echo()  # Add a newline for spacing
    if format == 'list':
        click.echo(click.style("Available datasets:", bold=True))
        for name in datasets:
            click.echo(f"- {name}")
    elif format == 'toml':
        click.echo(click.style("# Copy and paste the following into your config.toml under the [datasets] section:", bold=True))
        for name in datasets:
            click.echo(f'\n[datasets.{name}]')
            click.echo('primary_key = ["id"] # TODO: Replace with actual primary key(s)')
            # Generate a sensible default for the table name
            final_name = name.replace('-', '_')
            if final_name != name:
                click.echo(f'final_table_name = "{final_name}"')


@cli.command()
@click.pass_context
def validate(ctx):
    """Checks configuration and connectivity to the database and data source."""
    config = ctx.obj['CONFIG']
    click.echo("--- Running Configuration and Connection Validator ---")

    validator = ValidationService(config)
    results = validator.run_all_checks()

    all_successful = True
    for check_name, result in results.items():
        if result["success"]:
            status = click.style("SUCCESS", fg='green', bold=True)
        else:
            status = click.style("FAILED", fg='red', bold=True)
            all_successful = False

        message = result["message"]
        click.echo(f"[{status}] {check_name}: {message}")

    click.echo("--- Validation Complete ---")
    if not all_successful:
        click.secho("Validation failed. Please check your configuration and connections.", fg='red')
        raise click.Abort()
    else:
        click.secho("All checks passed successfully!", fg='green')


@cli.command()
@click.option('--version', help='Open Targets version to load. Defaults to the latest.')
@click.option('--staging-schema', default='staging', show_default=True, help='Schema for staging tables.')
@click.option('--final-schema', default='public', show_default=True, help='Schema for the final table.')
@click.option('--skip-confirmation', is_flag=True, help='Skip confirmation prompt for loading already loaded versions.')
@click.option('--no-continue-on-error', is_flag=True, help='Abort the process if an error occurs for any dataset.')
@click.option('--load-type', type=click.Choice(['delta', 'full-refresh'], case_sensitive=False), default='delta', show_default=True, help='Strategy for loading data.')
@click.argument('datasets', nargs=-1)
@click.pass_context
def load(ctx, version, staging_schema, final_schema, skip_confirmation, no_continue_on_error, load_type, datasets: Tuple[str]):
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
    click.echo(f"Load type: {click.style(load_type, bold=True)}")

    # Run validation checks before proceeding
    click.echo("Validating configuration and connections...")
    validator = ValidationService(config)
    results = validator.run_all_checks()
    all_successful = True
    for check_name, result in results.items():
        if not result["success"]:
            all_successful = False
            message = result["message"]
            click.secho(f"Validation FAILED for '{check_name}': {message}", fg='red')

    if not all_successful:
        click.secho("Prerequisite validation failed. Please check your configuration.", fg='red', bold=True)
        raise click.Abort()
    else:
        click.echo(click.style("Validation successful.", fg='green'))


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
            continue_on_error=not no_continue_on_error,
            load_type=load_type
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
