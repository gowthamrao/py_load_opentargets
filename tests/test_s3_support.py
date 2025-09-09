import pytest
import boto3
from moto import mock_aws

from src.py_load_opentargets.data_acquisition import list_available_versions


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_bucket(aws_credentials):
    """Create a mock S3 bucket and populate it with version-like folders."""
    with mock_aws():
        bucket_name = "mock-opentargets-bucket"
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket_name)

        # Create objects that simulate directories
        s3.put_object(Bucket=bucket_name, Key="platform/24.06/")
        s3.put_object(Bucket=bucket_name, Key="platform/24.09/")
        s3.put_object(Bucket=bucket_name, Key="platform/23.12/")
        s3.put_object(Bucket=bucket_name, Key="platform/README.md") # A file to be ignored

        yield bucket_name


def test_list_versions_from_s3(s3_bucket):
    """
    Verify that list_available_versions can discover versions from an S3 bucket.
    """
    # Arrange
    bucket_name = s3_bucket
    discovery_uri = f"s3://{bucket_name}/platform/"

    # Act
    versions = list_available_versions(discovery_uri)

    # Assert
    assert versions == ["24.09", "24.06", "23.12"]
