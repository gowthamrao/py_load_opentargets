import unittest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path

from py_load_opentargets.orchestrator import ETLOrchestrator

class TestETLOrchestrator(unittest.TestCase):

    def setUp(self):
        """Set up a mock loader and config for all tests."""
        self.mock_loader = MagicMock()
        self.mock_config = {
            'source': {
                'data_download_uri_template': 'gcs://fake-bucket/{version}/{dataset_name}/'
            },
            'datasets': {
                'targets': {
                    'primary_key': ['id'],
                    'final_table_name': 'targets_final'
                },
                'diseases': {
                    'primary_key': ['id']
                }
            }
        }
        self.version = "22.04"
        self.staging_schema = "staging"
        self.final_schema = "public"

    @patch('py_load_opentargets.orchestrator.download_dataset')
    def test_run_happy_path(self, mock_download):
        """Test a successful run for a single dataset."""
        # Arrange
        mock_download.return_value = Path("/fake/temp/dir/targets")
        self.mock_loader.get_last_successful_version.return_value = None
        self.mock_loader.table_exists.return_value = True

        orchestrator = ETLOrchestrator(
            config=self.mock_config,
            loader=self.mock_loader,
            datasets_to_process=['targets'],
            version=self.version,
            staging_schema=self.staging_schema,
            final_schema=self.final_schema
        )

        # Act
        orchestrator.run()

        # Assert
        # Verify download was called
        mock_download.assert_called_once_with(
            # The orchestrator passes the template, the download function formats it.
            'gcs://fake-bucket/{version}/{dataset_name}/', '22.04', 'targets', ANY
        )

        # Verify loader methods were called in order
        self.mock_loader.prepare_staging_schema.assert_called_once_with(self.staging_schema)
        self.mock_loader.prepare_staging_table.assert_called_once_with(
            f"{self.staging_schema}.targets", Path("/fake/temp/dir/targets")
        )
        self.mock_loader.bulk_load_native.assert_called_once()
        self.mock_loader.align_final_table_schema.assert_called_once()
        self.mock_loader.execute_merge_strategy.assert_called_once_with(
            f"{self.staging_schema}.targets", f"{self.final_schema}.targets_final", ['id']
        )
        self.mock_loader.update_metadata.assert_called_once_with(
            version=self.version, dataset='targets', success=True, row_count=ANY
        )
        # Verify staging table is dropped
        self.mock_loader.cursor.execute.assert_called_with("DROP TABLE IF EXISTS staging.targets;")

    @patch('py_load_opentargets.orchestrator.download_dataset')
    def test_run_error_handling(self, mock_download):
        """Test that an error during bulk load is handled correctly."""
        # Arrange
        mock_download.return_value = Path("/fake/temp/dir/diseases")
        self.mock_loader.get_last_successful_version.return_value = None
        # Simulate an error during the bulk load step
        self.mock_loader.bulk_load_native.side_effect = Exception("DB connection lost")

        orchestrator = ETLOrchestrator(
            config=self.mock_config,
            loader=self.mock_loader,
            datasets_to_process=['diseases'],
            version=self.version,
            staging_schema=self.staging_schema,
            final_schema=self.final_schema,
            continue_on_error=True # Ensure it continues
        )

        # Act
        orchestrator.run()

        # Assert
        # Verify it still tried to prepare the table
        self.mock_loader.prepare_staging_table.assert_called_once()

        # Verify the merge strategy was NOT called
        self.mock_loader.execute_merge_strategy.assert_not_called()

        # Verify metadata was updated with failure status
        self.mock_loader.update_metadata.assert_called_once_with(
            version=self.version, dataset='diseases', success=False, row_count=0, error_message="DB connection lost"
        )
        # Verify staging table is still dropped in the finally block
        self.mock_loader.cursor.execute.assert_called_with("DROP TABLE IF EXISTS staging.diseases;")

    def test_run_skips_already_loaded_version(self):
        """Test that the orchestrator skips a dataset if the same version is already loaded."""
        # Arrange
        # Simulate that the current version is already the last successful one
        self.mock_loader.get_last_successful_version.return_value = self.version

        orchestrator = ETLOrchestrator(
            config=self.mock_config,
            loader=self.mock_loader,
            datasets_to_process=['targets'],
            version=self.version,
            staging_schema=self.staging_schema,
            final_schema=self.final_schema,
            skip_confirmation=False # This is the default, but being explicit
        )

        # Act
        orchestrator.run()

        # Assert
        # Verify that no processing happened
        self.mock_loader.prepare_staging_table.assert_not_called()
        self.mock_loader.bulk_load_native.assert_not_called()
        self.mock_loader.update_metadata.assert_not_called()

if __name__ == '__main__':
    unittest.main()
