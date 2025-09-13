import pytest
from pathlib import Path
import os
import hashlib

from py_load_opentargets.data_acquisition import list_available_versions, get_checksum_manifest, download_dataset

# --- Configuration ---

# Using a known public FTP server for testing
FTP_HOST = "ftp.ebi.ac.uk"
FTP_PATH = "/pub/databases/opentargets/platform/"

# A specific version and a small dataset for testing
TEST_VERSION = "24.06" # A reasonably old but stable version

# --- Helper Functions ---

def calculate_sha1(filepath: Path) -> str:
    """Calculates the SHA1 hash of a file."""
    sha1 = hashlib.sha1()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(65536) # Read in 64k chunks
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()

# --- Integration Tests ---

@pytest.mark.integration
@pytest.mark.skip(reason="FTP server is flaky and causes intermittent test failures.")
def test_list_versions_from_real_ftp():
    """
    Connects to the real Open Targets FTP server and lists available versions.
    This tests FTP connectivity and the version parsing logic.
    """
    versions = list_available_versions(f"ftp://{FTP_HOST}{FTP_PATH}")
    assert versions is not None
    assert len(versions) > 0
    # Check if a known version format is present
    assert all(v.replace('.', '').isdigit() for v in versions)
    # Check for descending order
    assert versions == sorted(versions, reverse=True)

@pytest.mark.integration
@pytest.mark.skip(reason="FTP server is flaky and causes intermittent test failures.")
def test_checksum_manifest_download_and_parse(tmp_path: Path):
    """
    Tests that the checksum manifest can be downloaded and parsed correctly from the FTP server.
    """
    checksum_uri_template = f"ftp://{FTP_HOST}{FTP_PATH}{{version}}/"
    manifest = get_checksum_manifest(TEST_VERSION, checksum_uri_template)
    assert manifest is not None
    assert len(manifest) > 0
    # Check for a known key format
    assert any(k.startswith("output/etl/parquet/") for k in manifest.keys())
