import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from py_load_opentargets.data_acquisition import list_available_versions

@pytest.fixture
def mock_fsspec_ftp():
    """Fixture to mock the fsspec.filesystem for FTP."""
    # Patch the name where it is looked up, which is inside the data_acquisition module
    with patch('py_load_opentargets.data_acquisition.fsspec.filesystem') as mock_filesystem:
        mock_ftp = MagicMock()
        # Simulate the output of fs.ls()
        mock_ftp.ls.return_value = [
            'pub/databases/opentargets/platform/22.04',
            'pub/databases/opentargets/platform/22.06',
            'pub/databases/opentargets/platform/README.md',
            'pub/databases/opentargets/platform/latest',
        ]
        mock_filesystem.return_value = mock_ftp
        yield mock_filesystem

def test_list_available_versions_success(mock_fsspec_ftp):
    """Tests that versions are correctly parsed and sorted."""
    versions = list_available_versions()
    assert versions == ['22.06', '22.04']
    # Check that the mock was called correctly, with 'ftp' as a positional arg
    mock_fsspec_ftp.assert_called_with("ftp", host="ftp.ebi.ac.uk", anon=True)

def test_list_available_versions_failure(mock_fsspec_ftp):
    """Tests that an empty list is returned on failure."""
    # Configure the mock to raise an exception
    mock_fsspec_ftp.return_value.ls.side_effect = Exception("FTP connection failed")
    versions = list_available_versions()
    assert versions == []
