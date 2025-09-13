import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import hashlib
from py_load_opentargets.data_acquisition import list_available_versions, download_dataset, discover_datasets, _verify_file_checksum, get_remote_schema, get_remote_dataset_urls, verify_remote_dataset, _verify_remote_file_checksum, get_checksum_manifest

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

def test_discover_datasets_success(mock_fsspec):
    """Tests that datasets are correctly discovered from a listing."""
    mock_fsspec.ls.return_value = [
        {'name': 'mock_path/targets', 'type': 'directory'},
        {'name': 'mock_path/diseases', 'type': 'directory'},
        {'name': 'mock_path/manifest.txt', 'type': 'file'},
    ]
    datasets = discover_datasets("mock_uri/parquet/")
    assert datasets == ['diseases', 'targets']
    mock_fsspec.ls.assert_called_with('mock_path', detail=True)

def test_discover_datasets_failure(mock_fsspec):
    """Tests that an empty list is returned on discovery failure."""
    mock_fsspec.ls.side_effect = Exception("Listing failed")
    datasets = discover_datasets("mock_uri/parquet/")
    assert datasets == []

def test_discover_datasets_local(tmp_path):
    """Tests that datasets are correctly discovered from a local directory."""
    datasets_dir = tmp_path / "parquet"
    datasets_dir.mkdir()
    (datasets_dir / "targets").mkdir()
    (datasets_dir / "diseases").mkdir()
    (datasets_dir / "manifest.txt").touch()

    datasets = discover_datasets(str(datasets_dir))
    assert datasets == ['diseases', 'targets']

@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_success(mock_url_to_fs, tmp_path: Path):
    """Tests the successful download of a dataset."""
    mock_fs = MagicMock()
    mock_url_to_fs.return_value = (mock_fs, 'mock_remote_path')

    uri_template = "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}"
    version = "22.06"
    dataset = "targets"

    # Test with a valid manifest
    manifest = {
        "output/etl/parquet/targets/file1.parquet": "dummy_hash",
        "output/etl/parquet/targets/file2.parquet": "dummy_hash",
    }
    with patch('py_load_opentargets.data_acquisition._verify_file_checksum') as mock_verify:
        result_path = download_dataset(
            uri_template, version, dataset, tmp_path, checksum_manifest=manifest, max_workers=1
        )
        assert mock_verify.call_count == 2

    expected_local_path = tmp_path / version / dataset
    assert result_path == expected_local_path

    expected_calls = [
        call('gcs://open-targets/platform/22.06/output/etl/parquet/targets/file1.parquet', str(expected_local_path / 'file1.parquet')),
        call('gcs://open-targets/platform/22.06/output/etl/parquet/targets/file2.parquet', str(expected_local_path / 'file2.parquet')),
    ]
    mock_fs.get.assert_has_calls(expected_calls, any_order=True)


@patch('py_load_opentargets.data_acquisition.fsspec.core.url_to_fs')
def test_download_dataset_failure(mock_url_to_fs, tmp_path: Path):
    """Tests that an exception during download is raised."""
    mock_fs = MagicMock()
    mock_fs.get.side_effect = Exception("GCS download failed")
    mock_url_to_fs.return_value = (mock_fs, 'mock_remote_path')

    manifest = {"output/etl/parquet/targets/part-001.parquet": "hash"}
    with pytest.raises(Exception, match="GCS download failed"):
        download_dataset(
            "gcs://open-targets/platform/{version}/output/etl/parquet/{dataset_name}",
            "22.06",
            "targets",
            tmp_path,
            checksum_manifest=manifest,
            max_workers=2
        )

def test_verify_file_checksum_not_found(tmp_path):
    """Tests that FileNotFoundError is raised for a non-existent file."""
    non_existent_file = tmp_path / "non_existent.txt"
    with pytest.raises(FileNotFoundError):
        _verify_file_checksum(non_existent_file, "dummy_checksum")

def test_verify_file_checksum_mismatch(tmp_path):
    """Tests that ValueError is raised for a checksum mismatch."""
    file = tmp_path / "file.txt"
    file.write_text("content")
    with pytest.raises(ValueError):
        _verify_file_checksum(file, "wrong_checksum")

def test_get_remote_schema(tmp_path):
    """Tests that the schema is correctly inferred from a remote Parquet file."""
    schema = pa.schema([pa.field('foo', pa.int64())])
    table = pa.Table.from_pydict({'foo': [1, 2, 3]}, schema=schema)
    file_path = tmp_path / "test.parquet"
    pq.write_table(table, file_path)

    remote_url = f"file://{file_path}"
    inferred_schema = get_remote_schema([remote_url])
    assert inferred_schema == schema

def test_get_remote_schema_empty_list():
    """Tests that ValueError is raised for an empty list of URLs."""
    with pytest.raises(ValueError):
        get_remote_schema([])

def test_get_remote_schema_invalid_file(tmp_path):
    """Tests that an exception is raised for an invalid Parquet file."""
    file_path = tmp_path / "invalid.parquet"
    file_path.write_text("not a parquet file")
    remote_url = f"file://{file_path}"
    with pytest.raises(Exception):
        get_remote_schema([remote_url])

def test_get_remote_dataset_urls(tmp_path):
    """Tests that remote dataset URLs are correctly listed from a local directory."""
    version = "22.06"
    dataset = "targets"
    dataset_dir = tmp_path / version / dataset
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "file1.parquet").touch()
    (dataset_dir / "file2.parquet").touch()
    (dataset_dir / "manifest.txt").touch()

    uri_template = f"file://{tmp_path}/{{version}}/{{dataset_name}}"
    urls = get_remote_dataset_urls(uri_template, version, dataset)

    assert len(urls) == 2
    assert f"file://{dataset_dir}/file1.parquet" in urls
    assert f"file://{dataset_dir}/file2.parquet" in urls

def test_get_remote_dataset_urls_no_parquet(tmp_path):
    """Tests that an empty list is returned when no Parquet files are found."""
    version = "22.06"
    dataset = "targets"
    dataset_dir = tmp_path / version / dataset
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "manifest.txt").touch()

    uri_template = f"file://{tmp_path}/{{version}}/{{dataset_name}}"
    urls = get_remote_dataset_urls(uri_template, version, dataset)
    assert urls == []

def test_get_remote_dataset_urls_gcs():
    """Tests that an empty list is returned for GCS URIs."""
    urls = get_remote_dataset_urls("gs://bucket/{version}/{dataset_name}", "22.06", "targets")
    assert urls == []

@patch('py_load_opentargets.data_acquisition._verify_remote_file_checksum')
def test_verify_remote_dataset_sequential(mock_verify):
    """Tests sequential verification of remote dataset."""
    urls = ["file:///a.parquet", "file:///b.parquet"]
    dataset = "targets"
    manifest = {
        "output/etl/parquet/targets/a.parquet": "hash_a",
        "output/etl/parquet/targets/b.parquet": "hash_b",
    }
    verify_remote_dataset(urls, dataset, manifest, max_workers=1)
    mock_verify.assert_has_calls([
        call("file:///a.parquet", "hash_a"),
        call("file:///b.parquet", "hash_b"),
    ])

@patch('py_load_opentargets.data_acquisition._verify_remote_file_checksum')
def test_verify_remote_dataset_parallel(mock_verify):
    """Tests parallel verification of remote dataset."""
    urls = ["file:///a.parquet", "file:///b.parquet"]
    dataset = "targets"
    manifest = {
        "output/etl/parquet/targets/a.parquet": "hash_a",
        "output/etl/parquet/targets/b.parquet": "hash_b",
    }
    verify_remote_dataset(urls, dataset, manifest, max_workers=2)
    mock_verify.assert_has_calls([
        call("file:///a.parquet", "hash_a"),
        call("file:///b.parquet", "hash_b"),
    ], any_order=True)

def test_verify_remote_dataset_missing_checksum():
    """Tests that KeyError is raised for a missing checksum."""
    urls = ["file:///a.parquet"]
    dataset = "targets"
    manifest = {}
    with pytest.raises(KeyError):
        verify_remote_dataset(urls, dataset, manifest)

def test__verify_remote_file_checksum_success(tmp_path):
    """Tests successful verification of a remote file's checksum."""
    file_path = tmp_path / "file.txt"
    file_path.write_text("content")
    remote_url = f"file://{file_path}"
    expected_checksum = hashlib.sha1(b"content").hexdigest()
    _verify_remote_file_checksum(remote_url, expected_checksum)

def test__verify_remote_file_checksum_mismatch(tmp_path):
    """Tests checksum mismatch for a remote file."""
    file_path = tmp_path / "file.txt"
    file_path.write_text("content")
    remote_url = f"file://{file_path}"
    with pytest.raises(ValueError):
        _verify_remote_file_checksum(remote_url, "wrong_checksum")

def test__verify_remote_file_checksum_not_found():
    """Tests exception for a non-existent remote file."""
    with pytest.raises(FileNotFoundError):
        _verify_remote_file_checksum("file:///non_existent_file.txt", "")

def test_get_checksum_manifest_success(tmp_path):
    """Tests successful download and verification of a checksum manifest."""
    version = "22.06"
    release_dir = tmp_path / version
    release_dir.mkdir()
    manifest_content = "hash1 ./file1.txt\nhash2 ./file2.txt"
    manifest_path = release_dir / "release_data_integrity"
    manifest_path.write_text(manifest_content)

    manifest_checksum = hashlib.sha1(manifest_content.encode('utf-8')).hexdigest()
    checksum_path = release_dir / "release_data_integrity.sha1"
    checksum_path.write_text(f"{manifest_checksum}  ./release_data_integrity")

    uri_template = f"file://{tmp_path}/{{version}}"
    manifest = get_checksum_manifest(version, uri_template)

    assert manifest == {
        "file1.txt": "hash1",
        "file2.txt": "hash2",
    }

def test_get_checksum_manifest_mismatch(tmp_path):
    """Tests that a checksum mismatch for the manifest file raises an error."""
    version = "22.06"
    release_dir = tmp_path / version
    release_dir.mkdir()
    manifest_content = "hash1 ./file1.txt"
    manifest_path = release_dir / "release_data_integrity"
    manifest_path.write_text(manifest_content)

    checksum_path = release_dir / "release_data_integrity.sha1"
    checksum_path.write_text("wrong_checksum  ./release_data_integrity")

    uri_template = f"file://{tmp_path}/{{version}}"
    with pytest.raises(ValueError):
        get_checksum_manifest(version, uri_template)
