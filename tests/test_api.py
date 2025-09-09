import pytest
from unittest.mock import patch, MagicMock

from py_load_opentargets import api

# A sample config for mocking load_config
MOCK_CONFIG = {
    "source": {
        "version_discovery_uri": "mock://versions",
        "provider": "mock_provider",
        "mock_provider": {
            "data_uri_template": "mock://{version}/{dataset_name}"
        }
    },
    "database": {
        "backend": "postgres"
    },
    "datasets": {
        "dataset1": {"primary_key": ["id"]},
        "dataset2": {"primary_key": ["id"]},
    }
}

@patch("py_load_opentargets.api.setup_logging")
@patch("py_load_opentargets.api.ETLOrchestrator")
@patch("py_load_opentargets.api.ValidationService")
@patch("py_load_opentargets.api.load_config", return_value=MOCK_CONFIG)
def test_load_opentargets_api_direct_call(
    mock_load_config, mock_validator, mock_orchestrator, mock_setup_logging
):
    """
    Tests that the `load_opentargets` API function correctly calls the
    ETLOrchestrator with the provided arguments.
    """
    # Arrange
    # Mock the validator to simulate a successful validation
    mock_validator_instance = MagicMock()
    mock_validator_instance.run_all_checks.return_value = {"check1": {"success": True, "message": "OK"}}
    mock_validator.return_value = mock_validator_instance

    mock_orchestrator_instance = MagicMock()
    mock_orchestrator.return_value = mock_orchestrator_instance

    # Act
    api.load_opentargets(
        version="22.01",
        datasets=["dataset1"],
        config_path="/fake/path.toml",
        staging_schema="test_staging",
        final_schema="test_final",
        load_type="full-refresh",
        continue_on_error=False
    )

    # Assert
    # Verify config was loaded with the correct path
    mock_load_config.assert_called_once_with("/fake/path.toml")

    # Verify validator was used
    mock_validator.assert_called_once_with(MOCK_CONFIG)
    mock_validator_instance.run_all_checks.assert_called_once()

    # Verify orchestrator was instantiated with the correct parameters
    mock_orchestrator.assert_called_once_with(
        config=MOCK_CONFIG,
        datasets_to_process=["dataset1"],
        version="22.01",
        staging_schema="test_staging",
        final_schema="test_final",
        skip_confirmation=False,  # Default value
        continue_on_error=False,
        load_type="full-refresh",
    )

    # Verify the main execution method was called
    mock_orchestrator_instance.run.assert_called_once()


@patch("py_load_opentargets.api.setup_logging")
@patch("py_load_opentargets.api.list_available_versions", return_value=["23.06", "23.04"])
@patch("py_load_opentargets.api.ETLOrchestrator")
@patch("py_load_opentargets.api.ValidationService")
@patch("py_load_opentargets.api.load_config", return_value=MOCK_CONFIG)
def test_load_opentargets_api_version_discovery(
    mock_load_config, mock_validator, mock_orchestrator, mock_list_versions, mock_setup_logging
):
    """
    Tests that the API function performs version discovery when no version is provided
    and passes the latest version to the orchestrator.
    """
    # Arrange
    mock_validator_instance = MagicMock()
    mock_validator_instance.run_all_checks.return_value = {"check1": {"success": True, "message": "OK"}}
    mock_validator.return_value = mock_validator_instance

    # Act
    api.load_opentargets() # Call with no arguments to trigger defaults

    # Assert
    # Verify that version discovery was triggered
    mock_list_versions.assert_called_once_with(MOCK_CONFIG["source"]["version_discovery_uri"])

    # Verify that the latest version ("23.06") was passed to the orchestrator
    mock_orchestrator.assert_called_once()
    args, kwargs = mock_orchestrator.call_args
    assert kwargs["version"] == "23.06"
    # Verify it processes all datasets by default
    assert kwargs["datasets_to_process"] == ["dataset1", "dataset2"]


@patch("py_load_opentargets.api.setup_logging")
@patch("py_load_opentargets.api.ETLOrchestrator")
@patch("py_load_opentargets.api.ValidationService")
@patch("py_load_opentargets.api.load_config", return_value=MOCK_CONFIG)
def test_load_opentargets_api_validation_failure(
    mock_load_config, mock_validator, mock_orchestrator, mock_setup_logging
):
    """
    Tests that the API function raises a RuntimeError if validation fails.
    """
    # Arrange
    # Mock the validator to simulate a failed validation
    mock_validator_instance = MagicMock()
    mock_validator_instance.run_all_checks.return_value = {"check1": {"success": False, "message": "Failed"}}
    mock_validator.return_value = mock_validator_instance

    # Act & Assert
    with pytest.raises(RuntimeError, match="Prerequisite validation failed"):
        api.load_opentargets(version="22.01")

    # Verify that the orchestrator was NOT called
    mock_orchestrator.assert_not_called()
