import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from py_load_opentargets.data_acquisition import discover_datasets
from py_load_opentargets.cli import cli


@pytest.fixture
def mock_fsspec_ls():
    """Fixture to patch fsspec.ls"""
    with patch('fsspec.core.url_to_fs') as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_url_to_fs.return_value = (mock_fs, 'mock_path')
        yield mock_fs.ls

# Unit tests for discover_datasets function
@pytest.mark.parametrize(
    "ls_output, expected_datasets",
    [
        (
            [
                {'name': 'gs://bucket/path/targets', 'type': 'directory'},
                {'name': 'gs://bucket/path/diseases', 'type': 'directory'},
                {'name': 'gs://bucket/path/some_file.txt', 'type': 'file'},
            ],
            ['diseases', 'targets']
        ),
        ([], []), # Empty directory
        (
            [
                {'name': 'gs://bucket/path/some_file.txt', 'type': 'file'},
            ],
            [] # Directory with no subdirectories
        ),
    ]
)
def test_discover_datasets_success(mock_fsspec_ls, ls_output, expected_datasets):
    """Test discover_datasets successfully finds directories."""
    mock_fsspec_ls.return_value = ls_output
    datasets = discover_datasets('gs://any/path/')
    assert datasets == expected_datasets

def test_discover_datasets_fallback(mock_fsspec_ls):
    """Test fallback logic when 'type' is not in ls output."""
    mock_fsspec_ls.return_value = [
        {'name': 'gs://bucket/path/targets'},
        {'name': 'gs://bucket/path/diseases'},
        # This should be ignored
        {'name': 'gs://bucket/path/mock_path'}
    ]
    datasets = discover_datasets('gs://any/path/mock_path')
    assert datasets == ['diseases', 'targets']

def test_discover_datasets_exception(mock_fsspec_ls):
    """Test discover_datasets handles exceptions gracefully."""
    mock_fsspec_ls.side_effect = Exception("Storage error")
    datasets = discover_datasets('gs://any/path/')
    assert datasets == []

# Integration tests for CLI command
@patch('py_load_opentargets.data_acquisition.discover_datasets')
def test_cli_discover_datasets_list_format(mock_discover):
    """Test the CLI command with list format (default)."""
    mock_discover.return_value = ['diseases', 'targets']
    runner = CliRunner()
    result = runner.invoke(cli, ['discover-datasets', '--version', '24.06'])

    assert result.exit_code == 0
    assert "Available datasets:" in result.output
    assert "- diseases" in result.output
    assert "- targets" in result.output
    assert "[datasets.diseases]" not in result.output # Ensure TOML is not printed

@patch('py_load_opentargets.data_acquisition.discover_datasets')
def test_cli_discover_datasets_toml_format(mock_discover):
    """Test the CLI command with toml format."""
    mock_discover.return_value = ['diseases', 'association-by-datasrc']
    runner = CliRunner()
    result = runner.invoke(cli, ['discover-datasets', '--version', '24.06', '--format', 'toml'])

    assert result.exit_code == 0
    assert "# Copy and paste" in result.output
    # Test case 1: No hyphen, no final_table_name expected
    assert '[datasets.diseases]' in result.output
    assert 'primary_key = ["id"]' in result.output
    # Test case 2: Hyphenated, final_table_name is expected
    assert '[datasets.association-by-datasrc]' in result.output
    assert 'final_table_name = "association_by_datasrc"' in result.output

@patch('py_load_opentargets.data_acquisition.discover_datasets')
def test_cli_discover_datasets_no_results(mock_discover):
    """Test the CLI command when no datasets are found."""
    mock_discover.return_value = []
    runner = CliRunner()
    result = runner.invoke(cli, ['discover-datasets', '--version', '00.00'])

    assert result.exit_code == 0
    assert "Could not find any datasets" in result.output

def test_cli_discover_datasets_missing_version():
    """Test the CLI command fails if --version is not provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ['discover-datasets'])
    assert result.exit_code != 0
    assert "Missing option '--version'" in result.output
