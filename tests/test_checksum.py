import pytest
import hashlib
from unittest.mock import patch, MagicMock
from pathlib import Path

from py_load_opentargets.data_acquisition import get_checksum_manifest, download_dataset, _verify_file_checksum, _download_and_verify_one_file

# --- Fixtures ---

@pytest.fixture
def mock_fsspec_open():
    """
    Fixture to mock fsspec.core.url_to_fs and the subsequent fs.open call.
    Yields the mock filesystem and the mock file handle from fs.open.
    """
    with patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs') as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_url_to_fs.return_value = (mock_fs, 'mock_path')

        # fs.open() needs to return a context manager
        mock_file_context = MagicMock()
        mock_file_handle = MagicMock()
        mock_file_context.__enter__.return_value = mock_file_handle

        mock_fs.open.return_value = mock_file_context

        yield mock_fs, mock_file_handle

# --- Tests for get_checksum_manifest ---

def test_get_checksum_manifest_success(mock_fsspec_open):
    """Tests successful download, verification, and parsing of a checksum manifest."""
    mock_fs, mock_file_handle = mock_fsspec_open

    # Prepare mock data
    manifest_data = "abcde  ./output/etl/parquet/targets/file1.parquet\n12345  ./output/etl/parquet/diseases/file2.parquet"
    manifest_hash = hashlib.sha1(manifest_data.encode()).hexdigest()
    checksum_file_content = f"{manifest_hash}  release_data_integrity"

    # Configure the mock to return different content on consecutive reads
    mock_file_handle.read.side_effect = [
        checksum_file_content,
        manifest_data.encode('utf-8')
    ]

    # Call the function
    manifest = get_checksum_manifest("24.06", "ftp://fake/{version}/")

    # Assertions
    assert len(manifest) == 2
    assert manifest['output/etl/parquet/targets/file1.parquet'] == 'abcde'
    assert manifest['output/etl/parquet/diseases/file2.parquet'] == '12345'

    # Check that fsspec was called correctly
    mock_fs.open.assert_any_call("ftp://fake/24.06/release_data_integrity.sha1", 'r')
    mock_fs.open.assert_any_call("ftp://fake/24.06/release_data_integrity", 'rb')


def test_get_checksum_manifest_mismatch(mock_fsspec_open):
    """Tests that a ValueError is raised if the manifest checksum is incorrect."""
    mock_fs, mock_file_handle = mock_fsspec_open

    # Prepare mock data with a deliberate mismatch
    manifest_data = "some data"
    wrong_hash = "wrong_hash"
    checksum_file_content = f"{wrong_hash}  release_data_integrity"

    mock_file_handle.read.side_effect = [
        checksum_file_content,
        manifest_data.encode('utf-8')
    ]

    # Expect a ValueError
    with pytest.raises(ValueError, match="Checksum mismatch for manifest file"):
        get_checksum_manifest("24.06", "ftp://fake/{version}/")

# --- Tests for _verify_file_checksum ---

def test_verify_file_checksum_success(tmp_path):
    """Tests that a correct checksum passes verification."""
    dummy_file = tmp_path / "test.txt"
    file_content = b"hello world"
    dummy_file.write_bytes(file_content)

    expected_checksum = hashlib.sha1(file_content).hexdigest()

    # This should not raise an exception
    _verify_file_checksum(dummy_file, expected_checksum)


def test_verify_file_checksum_failure(tmp_path):
    """Tests that an incorrect checksum raises a ValueError."""
    dummy_file = tmp_path / "test.txt"
    dummy_file.write_text("hello world")

    wrong_checksum = "thisiswrong"

    with pytest.raises(ValueError, match="Checksum mismatch for"):
        _verify_file_checksum(dummy_file, wrong_checksum)

# --- Tests for download_dataset with checksums ---

@patch('py_load_opentargets.data_acquisition._verify_file_checksum')
@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_verifies_checksums(mock_url_to_fs, mock_verify, tmp_path):
    """
    Tests that download_dataset calls the verification function for each downloaded file.
    """
    # Setup mock fsspec
    mock_fs = MagicMock()
    mock_url_to_fs.return_value = (mock_fs, 'gcs://fake-bucket/24.06/targets')

    remote_files = ['gcs://fake-bucket/24.06/targets/file1.parquet']
    mock_fs.glob.return_value = remote_files

    # Setup test data
    file_content = b"dummy parquet data"
    file_hash = hashlib.sha1(file_content).hexdigest()

    version = "24.06"
    dataset = "targets"
    local_dataset_path = tmp_path / version / dataset
    local_dataset_path.mkdir(parents=True)
    (local_dataset_path / "file1.parquet").write_bytes(file_content)

    # The manifest key must match how it's constructed in the download function
    manifest_key = f"output/etl/parquet/{dataset}/file1.parquet"
    checksum_manifest = {manifest_key: file_hash}

    uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}/"

    # We mock fs.get to do nothing, since we're creating the file manually
    mock_fs.get.return_value = None

    # Call the function
    download_dataset(uri_template, version, dataset, tmp_path, checksum_manifest, max_workers=1)

    # Assert that the verification function was called with the correct arguments
    mock_verify.assert_called_once_with(
        local_dataset_path / "file1.parquet",
        file_hash
    )

@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_checksum_fail_raises_error(mock_url_to_fs, tmp_path):
    """
    Tests that if _verify_file_checksum raises an error, download_dataset propagates it.
    """
    # Setup mock fsspec
    mock_fs = MagicMock()
    mock_url_to_fs.return_value = (mock_fs, 'gcs://fake-bucket/24.06/targets')
    remote_files = ['gcs://fake-bucket/24.06/targets/file1.parquet']
    mock_fs.glob.return_value = remote_files

    # Setup test data where checksum will mismatch
    file_content = b"correct data"
    wrong_hash = "thisisnottherighthash"

    version = "24.06"
    dataset = "targets"
    local_dataset_path = tmp_path / version / dataset
    local_dataset_path.mkdir(parents=True)
    (local_dataset_path / "file1.parquet").write_bytes(file_content)

    manifest_key = f"output/etl/parquet/{dataset}/file1.parquet"
    checksum_manifest = {manifest_key: wrong_hash}

    uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}/"
    mock_fs.get.return_value = None

    # Expect a ValueError to be raised
    with pytest.raises(ValueError, match="Checksum mismatch"):
        download_dataset(uri_template, version, dataset, tmp_path, checksum_manifest, max_workers=1)


# --- Tests for the refactored helper function ---

def test_download_and_verify_one_file_success(tmp_path):
    """Tests the helper function for a successful download and verification."""
    mock_fs = MagicMock()
    # Create a dummy file content and its checksum
    file_content = b"test data"
    expected_checksum = hashlib.sha1(file_content).hexdigest()
    checksum_manifest = {"output/etl/parquet/dataset1/file1.parquet": expected_checksum}

    # Simulate the fs.get() by creating the file with the correct content
    mock_fs.get = MagicMock()
    def mock_get_side_effect(remote_path, local_path_str):
        Path(local_path_str).write_bytes(file_content)

    mock_fs.get.side_effect = mock_get_side_effect

    result_path = _download_and_verify_one_file(
        "remote/file1.parquet",
        tmp_path,
        "dataset1",
        checksum_manifest,
        mock_fs
    )
    assert result_path.exists()
    assert result_path.name == "file1.parquet"
    # Verify that fs.get was called
    mock_fs.get.assert_called_once()


def test_download_and_verify_one_file_key_error(tmp_path):
    """Tests that a KeyError is raised if the file is not in the manifest."""
    mock_fs = MagicMock()
    checksum_manifest = {}  # Empty manifest, so the key will be missing

    # Simulate fs.get()
    mock_fs.get = MagicMock()
    def mock_get_side_effect(remote_path, local_path_str):
        Path(local_path_str).write_text("test data")

    mock_fs.get.side_effect = mock_get_side_effect

    with pytest.raises(KeyError, match="Checksum not found in manifest for file"):
        _download_and_verify_one_file(
            "remote/file1.parquet",
            tmp_path,
            "dataset1",
            checksum_manifest,
            mock_fs
        )
