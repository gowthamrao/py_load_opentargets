# Python API Reference

While `py_load_opentargets` is primarily designed as a command-line tool, it also exposes a simple, high-level Python function for integration into other applications and workflows.

## Main Entry Point: `load_opentargets()`

The main function for programmatic execution is `py_load_opentargets.load_opentargets()`. This function is a wrapper around the entire ETL orchestration logic, providing a convenient, single entry point.

### Function Signature
The function is available at the top level of the package:
```python
from py_load_opentargets import load_opentargets

def load_opentargets(
    version: Optional[str] = None,
    datasets: Optional[List[str]] = None,
    config_path: Optional[str] = None,
    staging_schema: str = "staging",
    final_schema: str = "public",
    skip_confirmation: bool = False,
    continue_on_error: bool = True,
    load_type: str = "delta",
    json_logs: bool = False,
):
    # ...
```
For detailed information on the parameters, please refer to the function's docstring.

### Usage Example

Here is a basic example of how you might use the `load_opentargets` function in your own Python script.

!!! note
    Before running, ensure the `DB_CONN_STR` environment variable is set with your database connection string.
    ```bash
    export DB_CONN_STR="postgresql://user:pass@host:port/dbname"
    ```

```python
from py_load_opentargets import load_opentargets

try:
    # Example 1: Load the latest version of the 'targets' and 'diseases' datasets.
    # Assumes a default or user-configured config.toml file.
    print("--- Loading latest version of targets and diseases ---")
    load_opentargets(
        datasets=["targets", "diseases"],
        load_type="delta"
    )

    # Example 2: Load a specific version with a full refresh.
    # This will drop and recreate the final tables.
    print("--- Loading a specific version with a full refresh ---")
    load_opentargets(
        version="22.04",
        datasets=["evidence"],
        load_type="full-refresh",
        continue_on_error=False # Stop if the 'evidence' dataset fails
    )

    print("--- All ETL processes completed successfully! ---")

except Exception as e:
    print(f"An error occurred during the ETL process: {e}")

```
