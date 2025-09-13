import click
import logging
from typing import Tuple

from .config import load_config
from .data_acquisition import list_available_versions, discover_datasets
from .logging_utils import setup_logging
from .validator import ValidationService
from . import api

# Configure logging for the CLI
logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True),
    help="Path to a custom config.toml file.",
)
@click.option(
    "--json-logs",
    is_flag=True,
    help="Output logs in JSON format. Overrides config file setting.",
)
@click.pass_context
def cli(ctx, config_path, json_logs):
    """A CLI tool to download and load Open Targets data."""
    # The config is now loaded within the API/command function,
    # so we don't need to load it here anymore. We just pass the path along.
    ctx.ensure_object(dict)
    ctx.obj["CONFIG_PATH"] = config_path
    ctx.obj["JSON_LOGS"] = json_logs


@cli.command(name="list-versions")
@click.pass_context
def list_versions_cmd(ctx):
    """Lists the available Open Targets release versions."""
    config = load_config(ctx.obj["CONFIG_PATH"])
    source_config = config["source"]
    click.echo("Discovering available versions...")
    try:
        versions = list_available_versions(source_config["version_discovery_uri"])
        if versions:
            click.echo(click.style("Available versions (newest first):", bold=True))
            for version in versions:
                click.echo(f"- {version}")
        else:
            click.secho(
                "Could not find any available Open Targets versions.", fg="yellow"
            )
    except Exception as e:
        click.secho(f"Error during version discovery: {e}", fg="red")
        raise click.Abort()


@cli.command(name="discover-datasets")
@click.option("--version", required=True, help="The Open Targets version to inspect.")
@click.option(
    "--format",
    type=click.Choice(["list", "toml"], case_sensitive=False),
    default="list",
    show_default=True,
    help="Output format.",
)
@click.pass_context
def discover_datasets_cmd(ctx, version, format):
    """Discovers the available datasets for a given Open Targets version."""
    setup_logging(json_format=ctx.obj.get("JSON_LOGS", False))
    config = load_config(ctx.obj["CONFIG_PATH"])
    source_config = config["source"]

    # Prefer a dedicated URI template for discovering datasets.
    uri_template = source_config.get("dataset_discovery_uri_template")
    datasets_uri = ""
    if uri_template:
        datasets_uri = uri_template.format(version=version)
    else:
        # Fallback to deriving from the data_uri_template
        logger.warning(
            "`dataset_discovery_uri_template` not found in config, attempting to derive from `data_uri_template`."
        )
        data_uri_template = source_config.get("data_uri_template", "")
        if "{dataset_name}" not in data_uri_template:
            click.secho(
                "Error: 'data_uri_template' in config is missing the {{dataset_name}} placeholder.",
                fg="red",
            )
            raise click.Abort()
        # Get the base path before the dataset name placeholder
        datasets_uri = data_uri_template.split("{dataset_name}")[0].format(
            version=version
        )

    click.echo(
        f"Discovering datasets for version {click.style(version, bold=True)} at {datasets_uri}..."
    )

    datasets = discover_datasets(datasets_uri)

    if not datasets:
        click.secho(
            "Could not find any datasets for this version. Check the version number and your source configuration.",
            fg="yellow",
        )
        return

    click.echo()  # Add a newline for spacing
    if format == "list":
        click.echo(click.style("Available datasets:", bold=True))
        for name in datasets:
            click.echo(f"- {name}")
    elif format == "toml":
        click.echo(
            click.style(
                "# Copy and paste the following into your config.toml under the [datasets] section:",
                bold=True,
            )
        )
        for name in datasets:
            click.echo(f"\n[datasets.{name}]")
            click.echo(
                'primary_key = ["id"] # TODO: Replace with actual primary key(s)'
            )
            # Generate a sensible default for the table name
            final_name = name.replace("-", "_")
            if final_name != name:
                click.echo(f'final_table_name = "{final_name}"')


@cli.command()
@click.pass_context
def validate(ctx):
    """Checks configuration and connectivity to the database and data source."""
    setup_logging(json_format=ctx.obj.get("JSON_LOGS", False))
    config = load_config(ctx.obj["CONFIG_PATH"])
    click.echo("--- Running Configuration and Connection Validator ---")

    validator = ValidationService(config)
    results = validator.run_all_checks()

    all_successful = True
    for check_name, result in results.items():
        if result["success"]:
            status = click.style("SUCCESS", fg="green", bold=True)
        else:
            status = click.style("FAILED", fg="red", bold=True)
            all_successful = False

        message = result["message"]
        click.echo(f"[{status}] {check_name}: {message}")

    click.echo("--- Validation Complete ---")
    if not all_successful:
        click.secho(
            "Validation failed. Please check your configuration and connections.",
            fg="red",
        )
        raise click.Abort()
    else:
        click.secho("All checks passed successfully!", fg="green")


@cli.command()
@click.option("--version", help="Open Targets version to load. Defaults to the latest.")
@click.option(
    "--staging-schema",
    default="staging",
    show_default=True,
    help="Schema for staging tables.",
)
@click.option(
    "--final-schema",
    default="public",
    show_default=True,
    help="Schema for the final table.",
)
@click.option(
    "--skip-confirmation",
    is_flag=True,
    help="Skip confirmation prompt for loading already loaded versions.",
)
@click.option(
    "--no-continue-on-error",
    is_flag=True,
    help="Abort the process if an error occurs for any dataset.",
)
@click.option(
    "--load-type",
    type=click.Choice(["delta", "full-refresh"], case_sensitive=False),
    default="delta",
    show_default=True,
    help="Strategy for loading data.",
)
@click.argument("datasets", nargs=-1)
@click.pass_context
def load(
    ctx,
    version,
    staging_schema,
    final_schema,
    skip_confirmation,
    no_continue_on_error,
    load_type,
    datasets: Tuple[str],
):
    """
    Downloads and loads specified Open Targets datasets into a database.

    This command orchestrates the full ETL process for each specified dataset.
    If no datasets are provided, it will process ALL datasets defined in the
    configuration file.
    """
    try:
        # The --json-logs flag on the `cli` group is the source of truth for the API call.
        json_logs = ctx.obj.get("JSON_LOGS", False)
        config_path = ctx.obj.get("CONFIG_PATH")

        # The API function now handles all the orchestration logic.
        # The CLI's role is simply to gather arguments and call the API.
        api.load_opentargets(
            version=version,
            datasets=list(datasets) or None,  # Convert tuple to list
            config_path=config_path,
            staging_schema=staging_schema,
            final_schema=final_schema,
            skip_confirmation=skip_confirmation,
            continue_on_error=(not no_continue_on_error),
            load_type=load_type,
            json_logs=json_logs,
        )

    except Exception as e:
        # The API function raises exceptions on failure, which we catch here
        # to provide a clean exit for the CLI user. The API's own logging
        # will have already recorded the detailed error.
        click.secho(f"A fatal error occurred: {e}", fg="red")
        raise click.Abort()
    finally:
        click.echo("\n--- CLI Process Finished ---")


if __name__ == "__main__":
    cli()
