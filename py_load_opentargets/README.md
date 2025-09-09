# py_load_opentargets

A Python package to download and load Open Targets data.

## Installation

```bash
pip install py_load_opentargets
```

## Usage

This package provides a simple way to download and load datasets from the Open Targets platform.

### List available releases

You can get a list of all available data releases:

```python
from py_load_opentargets import core

releases = core.get_releases()
print(releases)
```

### List available datasets for a release

Once you have a release version, you can get a list of all available datasets for that release:

```python
from py_load_opentargets import core

datasets = core.get_datasets("25.06")
print(datasets)
```

### Download a dataset

You can download a specific dataset to a local directory.

```python
from py_load_opentargets import core

# Download the 'disease' dataset from release 25.06 to the current directory
core.download_dataset("25.06", "disease", path=".")
```

This will create a directory named `disease` in the specified path, containing the downloaded Parquet files.

### Load a dataset

After downloading a dataset, you can load it into a pandas DataFrame:

```python
from py_load_opentargets import core

# Load the 'disease' dataset from the local path
df = core.load_dataset("disease")
print(df.head())
```

## Development

To install the package for development, clone the repository and install it in editable mode with test dependencies:

```bash
git clone https://github.com/example/py_load_opentargets.git
cd py_load_opentargets
pip install -e .[test]
```

To run the tests:

```bash
pytest
```
