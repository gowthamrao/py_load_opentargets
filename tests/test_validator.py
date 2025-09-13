import pytest
import psycopg
from unittest.mock import MagicMock

from py_load_opentargets.validator import ValidationService


@pytest.fixture
def minimal_config():
    """A minimal, valid config for testing."""
    return {
        "source": {"version_discovery_uri": "gs://open-targets/platform/"},
        "datasets": {
            "targets": {"primary_key": ["id"]},
            "diseases": {"primary_key": ["id"]},
        },
    }


def test_check_db_connection_success(mocker):
    """Test successful database connection validation."""
    mocker.patch("os.getenv", return_value="postgresql://user:pass@host/db")
    mock_connect = mocker.patch("psycopg.connect")
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value.__enter__.return_value = mock_conn

    validator = ValidationService({})
    result = validator.check_db_connection()

    assert result["success"] is True
    assert "successful" in result["message"]


def test_check_db_connection_failure_no_env(mocker):
    """Test failure when DB_CONN_STR is not set."""
    mocker.patch("os.getenv", return_value=None)
    validator = ValidationService({})
    result = validator.check_db_connection()

    assert result["success"] is False
    assert "not set" in result["message"]


def test_check_db_connection_failure_exception(mocker):
    """Test failure when psycopg.connect raises an exception."""
    mocker.patch("os.getenv", return_value="postgresql://user:pass@host/db")
    mocker.patch(
        "psycopg.connect", side_effect=psycopg.OperationalError("Test connection error")
    )

    validator = ValidationService({})
    result = validator.check_db_connection()

    assert result["success"] is False
    assert "Test connection error" in result["message"]


def test_check_source_connection_success(mocker, minimal_config):
    """Test successful source connection validation."""
    mocker.patch(
        "py_load_opentargets.validator.list_available_versions",
        return_value=["24.06", "24.03"],
    )
    validator = ValidationService(minimal_config)
    result = validator.check_source_connection()

    assert result["success"] is True
    assert "24.06" in result["message"]


def test_check_source_connection_failure_no_versions(mocker, minimal_config):
    """Test failure when no versions are found."""
    mocker.patch(
        "py_load_opentargets.validator.list_available_versions", return_value=[]
    )
    validator = ValidationService(minimal_config)
    result = validator.check_source_connection()

    assert result["success"] is False
    assert "Could not find any versions" in result["message"]


def test_check_source_connection_failure_exception(mocker, minimal_config):
    """Test failure when list_available_versions raises an exception."""
    mocker.patch(
        "py_load_opentargets.validator.list_available_versions",
        side_effect=Exception("Test source error"),
    )
    validator = ValidationService(minimal_config)
    result = validator.check_source_connection()

    assert result["success"] is False
    assert "Test source error" in result["message"]


def test_check_dataset_definitions_success(minimal_config):
    """Test successful dataset definition validation."""
    validator = ValidationService(minimal_config)
    result = validator.check_dataset_definitions()

    assert result["success"] is True
    assert "valid" in result["message"]


def test_check_dataset_definitions_failure_missing_pk():
    """Test failure when a dataset is missing a primary_key."""
    invalid_config = {
        "datasets": {
            "targets": {"primary_key": ["id"]},
            "diseases": {},  # Missing primary_key
        }
    }
    validator = ValidationService(invalid_config)
    result = validator.check_dataset_definitions()

    assert result["success"] is False
    assert "diseases" in result["message"]
    assert "targets" not in result["message"]


def test_check_dataset_definitions_failure_no_datasets():
    """Test failure when the datasets section is empty."""
    invalid_config = {"datasets": {}}
    validator = ValidationService(invalid_config)
    result = validator.check_dataset_definitions()

    assert result["success"] is False
    assert "missing or empty" in result["message"]
