import pytest
import boto3
import toml
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws
from pathlib import Path
from unittest.mock import patch, MagicMock

from py_load_opentargets.orchestrator import ETLOrchestrator
from py_load_opentargets.loader import DatabaseLoader

TEST_BUCKET = "test-opentargets-bucket"
TEST_VERSION = "24.06"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Yields a boto3 S3 client in a mocked AWS environment."""
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def mock_s3_data(s3_client):
    """
    Sets up a mock S3 bucket and populates it with a dummy Open Targets structure.
    """
    s3_client.create_bucket(Bucket=TEST_BUCKET)

    # 1. Create dummy parquet file for 'targets' dataset
    dummy_schema = pa.schema([pa.field("id", pa.string())])
    dummy_table = pa.Table.from_pydict(
        {"id": ["target1", "target2"]}, schema=dummy_schema
    )
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(dummy_table, parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue().to_pybytes()
    parquet_hash = hashlib.sha1(parquet_bytes).hexdigest()

    parquet_key = f"platform/{TEST_VERSION}/output/etl/parquet/targets/part-0.parquet"
    s3_client.put_object(Bucket=TEST_BUCKET, Key=parquet_key, Body=parquet_bytes)

    # 2. Create checksum manifest
    manifest_content = f"{parquet_hash}  ./output/etl/parquet/targets/part-0.parquet"
    manifest_bytes = manifest_content.encode("utf-8")
    manifest_hash = hashlib.sha1(manifest_bytes).hexdigest()

    manifest_key = f"platform/{TEST_VERSION}/release_data_integrity"
    s3_client.put_object(Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest_bytes)

    # 3. Create checksum for the manifest
    manifest_checksum_key = f"platform/{TEST_VERSION}/release_data_integrity.sha1"
    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=manifest_checksum_key,
        Body=f"{manifest_hash}".encode("utf-8"),
    )

    # 4. Create version discovery file (empty file to make the "directory" listable)
    version_key = f"platform/{TEST_VERSION}/"
    s3_client.put_object(Bucket=TEST_BUCKET, Key=version_key, Body=b"")


@pytest.fixture
def mock_db_loader_factory():
    """Fixture to provide a mock DatabaseLoader factory."""
    mock_loader_instance = MagicMock(spec=DatabaseLoader)
    mock_loader_instance.get_last_successful_version.return_value = None
    # Mock bulk_load_native to return the number of rows from the dummy data
    mock_loader_instance.bulk_load_native.return_value = 2

    def factory():
        return mock_loader_instance

    with patch(
        "py_load_opentargets.orchestrator.get_db_loader_factory"
    ) as mock_get_factory:
        mock_get_factory.return_value = factory
        yield mock_loader_instance


@pytest.fixture
def s3_test_config(tmp_path: Path) -> Path:
    """Creates a temporary config file pointing to the mock S3 bucket."""
    config_data = {
        "source": {
            "provider": "s3",
            "s3": {
                "version_discovery_uri": f"s3://{TEST_BUCKET}/platform/",
                "checksum_uri_template": f"s3://{TEST_BUCKET}/platform/{{version}}/",
                "data_uri_template": f"s3://{TEST_BUCKET}/platform/{{version}}/output/etl/parquet/{{dataset_name}}/",
                "data_download_uri_template": f"s3://{TEST_BUCKET}/platform/{{version}}/output/etl/parquet/{{dataset_name}}/",
            },
        },
        "execution": {
            "max_workers": 1,
            "load_strategy": "stream",  # Use stream to test remote reading
        },
        "database": {"backend": "postgres"},
        "datasets": {"targets": {"primary_key": ["id"]}},
    }
    config_file = tmp_path / "s3_config.toml"
    with open(config_file, "w", encoding="utf-8") as f:
        f.write(toml.dumps(config_data))
    return config_file


@patch("py_load_opentargets.orchestrator.get_remote_schema")
@patch("py_load_opentargets.orchestrator.get_remote_dataset_urls")
@patch("py_load_opentargets.orchestrator.verify_remote_dataset")
@patch("py_load_opentargets.orchestrator.get_checksum_manifest")
def test_s3_orchestration_e2e(
    mock_get_checksum,
    mock_verify_remote,
    mock_get_urls,
    mock_get_schema,
    s3_client,
    mock_s3_data,
    s3_test_config,
    mock_db_loader_factory,
):
    """
    Tests the full ETL orchestration using the mock S3 bucket.
    This test mocks out the data_acquisition functions to isolate the orchestrator
    and avoid complex dependency issues with moto and async libraries.
    """
    # Mock the data acquisition functions
    mock_get_checksum.return_value = {
        "output/etl/parquet/targets/part-0.parquet": "dummy_hash"
    }
    mock_get_urls.return_value = [
        f"s3://{TEST_BUCKET}/platform/{TEST_VERSION}/output/etl/parquet/targets/part-0.parquet"
    ]
    mock_get_schema.return_value = pa.schema([pa.field("id", pa.string())])
    mock_verify_remote.return_value = None  # It returns nothing on success

    # Set DB_CONN_STR as it's required by the orchestrator
    import os

    os.environ["DB_CONN_STR"] = "postgresql://user:pass@host/db"

    from py_load_opentargets.config import load_config

    # Load the temporary S3-specific config
    config = load_config(s3_test_config)

    orchestrator = ETLOrchestrator(
        config=config,
        datasets_to_process=["targets"],
        version=TEST_VERSION,
        staging_schema="staging",
        final_schema="public",
        load_type="delta",
    )

    # Run the orchestration
    orchestrator.run()

    # --- Assertions ---
    # 1. Assert that the database loader was connected
    mock_db_loader_factory.connect.assert_called_once()

    # 2. Assert that the staging table was prepared with the correct schema
    mock_db_loader_factory.prepare_staging_table.assert_called_once()
    call_args = mock_db_loader_factory.prepare_staging_table.call_args
    assert call_args[0][0] == "staging.targets"
    schema = call_args[0][1]
    assert schema.names == ["id"]

    # 3. Assert that bulk_load_native was called with the correct S3 URI
    mock_db_loader_factory.bulk_load_native.assert_called_once()
    bulk_load_args = mock_db_loader_factory.bulk_load_native.call_args
    loaded_uris = bulk_load_args[0][1]
    assert len(loaded_uris) == 1
    assert loaded_uris[0].startswith(
        f"s3://{TEST_BUCKET}/platform/{TEST_VERSION}/output/etl/parquet/targets/part-0.parquet"
    )

    # 4. Assert that the merge strategy was executed
    mock_db_loader_factory.execute_merge_strategy.assert_called_once()

    # 5. Assert that metadata was updated successfully
    mock_db_loader_factory.update_metadata.assert_called_once()
    metadata_kwargs = mock_db_loader_factory.update_metadata.call_args.kwargs
    assert metadata_kwargs["version"] == TEST_VERSION
    assert metadata_kwargs["dataset"] == "targets"
    assert metadata_kwargs["success"] is True
    assert (
        metadata_kwargs["row_count"] == 2
    )  # From the dummy parquet file and mock return
    assert metadata_kwargs.get("error_message") is None
