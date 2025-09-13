import pytest
import hashlib
from unittest.mock import patch, mock_open

# Functions to test
from py_load_opentargets.data_acquisition import (
    _verify_remote_file_checksum,
    verify_remote_dataset,
)

# --- Fixtures ---


@pytest.fixture
def mock_fsspec_open_for_remote_verify():
    """Mocks fsspec.open for testing _verify_remote_file_checksum."""
    # Use mock_open to create a file-like object that can be used in a `with` statement
    m = mock_open(read_data=b"initial data")
    with patch("fsspec.open", m) as mock_spec_open:
        yield mock_spec_open


# --- Tests for _verify_remote_file_checksum ---


def test_verify_remote_file_checksum_success(mock_fsspec_open_for_remote_verify):
    """Tests that a correct remote checksum passes verification."""
    file_content = b"hello remote world"
    expected_checksum = hashlib.sha1(file_content).hexdigest()

    # Configure the mock to return the correct content
    mock_fsspec_open_for_remote_verify.return_value.read.side_effect = [
        file_content,
        b"",
    ]

    # This should not raise an exception
    _verify_remote_file_checksum("gcs://fake/file.txt", expected_checksum)
    mock_fsspec_open_for_remote_verify.assert_called_once_with(
        "gcs://fake/file.txt", "rb"
    )


def test_verify_remote_file_checksum_failure(mock_fsspec_open_for_remote_verify):
    """Tests that an incorrect remote checksum raises a ValueError."""
    file_content = b"this is the wrong content"
    wrong_checksum = "definitelynottherightchecksum"

    mock_fsspec_open_for_remote_verify.return_value.read.side_effect = [
        file_content,
        b"",
    ]

    with pytest.raises(ValueError, match="Checksum mismatch for remote file"):
        _verify_remote_file_checksum("gcs://fake/file.txt", wrong_checksum)


# --- Tests for verify_remote_dataset ---


@patch("py_load_opentargets.data_acquisition._verify_remote_file_checksum")
def test_verify_remote_dataset_success(mock_verify_remote):
    """Tests that the orchestrator calls the verification function for each URL."""
    urls = ["gcs://bucket/file1.parquet", "gcs://bucket/file2.parquet"]
    dataset = "my_dataset"
    manifest = {
        "output/etl/parquet/my_dataset/file1.parquet": "hash1",
        "output/etl/parquet/my_dataset/file2.parquet": "hash2",
    }

    verify_remote_dataset(urls, dataset, manifest, max_workers=2)

    # Assert that the underlying verification function was called for each file
    assert mock_verify_remote.call_count == 2
    mock_verify_remote.assert_any_call("gcs://bucket/file1.parquet", "hash1")
    mock_verify_remote.assert_any_call("gcs://bucket/file2.parquet", "hash2")


@patch("py_load_opentargets.data_acquisition._verify_remote_file_checksum")
def test_verify_remote_dataset_raises_on_checksum_mismatch(mock_verify_remote):
    """Tests that an exception from the worker is propagated."""
    urls = ["gcs://bucket/file1.parquet"]
    dataset = "my_dataset"
    manifest = {"output/etl/parquet/my_dataset/file1.parquet": "hash1"}

    # Configure the mock to simulate a verification failure
    mock_verify_remote.side_effect = ValueError("Checksum mismatch")

    with pytest.raises(ValueError, match="Checksum mismatch"):
        verify_remote_dataset(urls, dataset, manifest, max_workers=1)


def test_verify_remote_dataset_raises_on_missing_key():
    """Tests that a KeyError is raised if a checksum is missing from the manifest."""
    urls = ["gcs://bucket/file_not_in_manifest.parquet"]
    dataset = "my_dataset"
    manifest = {"output/etl/parquet/my_dataset/some_other_file.parquet": "hash1"}

    with pytest.raises(KeyError, match="Checksum not found in manifest"):
        verify_remote_dataset(urls, dataset, manifest)
