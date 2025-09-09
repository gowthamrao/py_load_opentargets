# Python API Reference

While `py-load-opentargets` is primarily designed as a command-line tool, it also exposes a Python API for integration into other applications and workflows.

## Main Entry Point: `ETLOrchestrator`

The main class for programmatic execution is `py_load_opentargets.orchestrator.ETLOrchestrator`.

### Class Signature
```python
class ETLOrchestrator:
    def __init__(
        self,
        config: Dict[str, Any],
        datasets_to_process: List[str],
        version: str,
        staging_schema: str,
        final_schema: str,
        skip_confirmation: bool = False,
        continue_on_error: bool = True,
        load_type: str = "delta",
    ):
        # ...

    def run(self):
        # ...
```

### Usage Example

Here is a basic example of how you might use the orchestrator in your own Python script.

```python
import os
import toml
from py_load_opentargets.orchestrator import ETLOrchestrator
from py_load_opentargets.data_acquisition import list_available_versions

# 1. Set the database connection string
os.environ["DB_CONN_STR"] = "postgresql://user:pass@host:port/dbname"

# 2. Load the configuration from a file
with open("config.toml", "r") as f:
    config = toml.load(f)

# 3. Discover the latest version
latest_version = list_available_versions(config['source']['gcs_base_url'])[0]

# 4. Define which datasets to process
datasets = ["targets", "diseases"]

# 5. Initialize the orchestrator
orchestrator = ETLOrchestrator(
    config=config,
    datasets_to_process=datasets,
    version=latest_version,
    staging_schema="staging",
    final_schema="public",
    load_type="delta" # or "full-refresh"
)

# 6. Run the ETL process
orchestrator.run()
```

This provides a powerful way to embed the Open Targets loading logic directly within a larger data processing pipeline.
