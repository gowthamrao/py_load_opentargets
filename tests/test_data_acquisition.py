import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from py_load_opentargets.data_acquisition import list_available_versions, download_dataset

@pytest.fixture
def mock_fsspec_ftp():
    """Fixture to mock the fsspec.filesystem for FTP."""
    with patch('py_load_opentargets.data_acquisition.fsspec.filesystem') as mock_filesystem:
        mock_ftp = MagicMock()
        mock_ftp.ls.return_value = [
            'pub/databases/opentargets/platform/22.04',
            'pub/databases/opentargets/platform/22.06',
            'pub/databases/opentargets/platform/README.md',
        ]
        mock_filesystem.return_value = mock_ftp
        yield mock_filesystem

def test_list_available_versions_success(mock_fsspec_ftp):
    """Tests that versions are correctly parsed and sorted."""
    versions = list_available_versions(ftp_host="fake.host", ftp_path="/fake/path")
    assert versions == ['22.06', '22.04']
    mock_fsspec_ftp.assert_called_with("ftp", host="fake.host", anon=True)
    mock_fsspec_ftp.return_value.ls.assert_called_with("/fake/path", detail=False)

def test_list_available_versions_failure(mock_fsspec_ftp):
    """Tests that an empty list is returned on failure."""
    mock_fsspec_ftp.return_value.ls.side_effect = Exception("FTP connection failed")
    versions = list_available_versions(ftp_host="fake.host", ftp_path="/fake/path")
    assert versions == []

@patch('py_load_opentargets.data_acquisition.fsspec.filesystem')
def test_download_dataset_success(mock_filesystem, tmp_path: Path):
    """Tests the successful download of a dataset."""
    mock_gcs = MagicMock()
    mock_filesystem.return_value = mock_gcs

    gcs_url = "gs://fake-bucket/"
    version = "22.06"
    dataset = "targets"

    result_path = download_dataset(gcs_url, version, dataset, tmp_path)

    expected_source_url = f"{gcs_url}{version}/output/etl/parquet/{dataset}/"
    expected_local_path = tmp_path / version / dataset

    assert result_path == expected_local_path
    mock_filesystem.assert_called_with("gcs", anon=True)
    mock_gcs.get.assert_called_once_with(expected_source_url, str(expected_local_path), recursive=True)

@patch('py_load_opentargets.data_acquisition.fsspec.filesystem')
def test_download_dataset_failure(mock_filesystem, tmp_path: Path):
    """Tests that an exception during download is raised."""
    mock_gcs = MagicMock()
    mock_gcs.get.side_effect = Exception("GCS download failed")
    mock_filesystem.return_value = mock_gcs

    with pytest.raises(Exception, match="GCS download failed"):
        download_dataset("gs://fake-bucket/", "22.06", "targets", tmp_path)
