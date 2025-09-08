import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from py_load_opentargets.data_acquisition import list_available_versions, download_dataset

@pytest.fixture
def mock_fsspec():
    """Fixture to mock fsspec.core.url_to_fs."""
    with patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs') as mock_url_to_fs:
        mock_fs = MagicMock()
        mock_url_to_fs.return_value = (mock_fs, 'mock_path')
        yield mock_fs

def test_list_available_versions_success(mock_fsspec):
    """Tests that versions are correctly parsed and sorted."""
    mock_fsspec.ls.return_value = [
        'ftp://fake.host/fake/path/22.04',
        'ftp://fake.host/fake/path/22.06',
        'ftp://fake.host/fake/path/README.md',
    ]
    # Simulate that the first two are directories
    mock_fsspec.isdir.side_effect = [True, True, False]

    versions = list_available_versions("ftp://fake.host/fake/path")
    assert versions == ['22.06', '22.04']
    mock_fsspec.ls.assert_called_with('mock_path', detail=False)

def test_list_available_versions_failure(mock_fsspec):
    """Tests that an empty list is returned on failure."""
    mock_fsspec.ls.side_effect = Exception("Connection failed")
    versions = list_available_versions("ftp://fake.host/fake/path")
    assert versions == []

@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_sequential_success(mock_url_to_fs, tmp_path: Path):
    """Tests the successful download of a dataset sequentially when max_workers=1."""
    mock_fs = MagicMock()
    mock_url_to_fs.return_value = (mock_fs, 'mock_remote_path')
    mock_fs.glob.return_value = ['mock_remote_path/file1.parquet']

    uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}/"
    version = "22.06"
    dataset = "targets"

    # Test with a valid manifest
    manifest = {
        "output/etl/parquet/targets/file1.parquet": "dummy_hash"
    }
    with patch('py_load_opentargets.data_acquisition._verify_file_checksum') as mock_verify:
        result_path = download_dataset(
            uri_template, version, dataset, tmp_path, checksum_manifest=manifest, max_workers=1
        )
        mock_verify.assert_called_once()

    expected_local_path = tmp_path / version / dataset
    assert result_path == expected_local_path
    mock_fs.glob.assert_called_once_with("mock_remote_path/*.parquet")
    # It now calls get for each file, not recursively for the directory
    mock_fs.get.assert_called_once_with('mock_remote_path/file1.parquet', str(expected_local_path / 'file1.parquet'))


@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_parallel_success(mock_url_to_fs, tmp_path: Path):
    """Tests the successful parallel download of multiple files."""
    mock_fs = MagicMock()
    mock_url_to_fs.return_value = (mock_fs, 'gcs://fake-bucket/22.06/targets')

    remote_files = [
        'gcs://fake-bucket/22.06/targets/part-001.parquet',
        'gcs://fake-bucket/22.06/targets/part-002.parquet',
        'gcs://fake-bucket/22.06/targets/part-003.parquet',
    ]
    mock_fs.glob.return_value = remote_files

    uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}/"
    version = "22.06"
    dataset = "targets"

    manifest = {
        f"output/etl/parquet/targets/{Path(f).name}": "dummy_hash" for f in remote_files
    }
    with patch('py_load_opentargets.data_acquisition._verify_file_checksum') as mock_verify:
        result_path = download_dataset(
            uri_template, version, dataset, tmp_path, checksum_manifest=manifest, max_workers=4
        )
        assert mock_verify.call_count == len(remote_files)


    expected_local_path = tmp_path / version / dataset
    assert result_path == expected_local_path
    mock_fs.glob.assert_called_once_with("gcs://fake-bucket/22.06/targets/*.parquet")
    expected_calls = [
        call(remote_files[0], str(expected_local_path / 'part-001.parquet')),
        call(remote_files[1], str(expected_local_path / 'part-002.parquet')),
        call(remote_files[2], str(expected_local_path / 'part-003.parquet')),
    ]
    mock_fs.get.assert_has_calls(expected_calls, any_order=True)
    assert mock_fs.get.call_count == len(remote_files)


@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_failure(mock_url_to_fs, tmp_path: Path):
    """Tests that an exception during download is raised."""
    mock_fs = MagicMock()
    mock_fs.get.side_effect = Exception("GCS download failed")
    mock_url_to_fs.return_value = (mock_fs, 'mock_remote_path')
    mock_fs.glob.return_value = ['gcs://fake-bucket/22.06/targets/part-001.parquet']

    manifest = {"output/etl/parquet/targets/part-001.parquet": "hash"}
    with pytest.raises(Exception, match="GCS download failed"):
        download_dataset(
            "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}/",
            "22.06",
            "targets",
            tmp_path,
            checksum_manifest=manifest,
            max_workers=2
        )
