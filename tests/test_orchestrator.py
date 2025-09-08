import unittest
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path

from py_load_opentargets.orchestrator import ETLOrchestrator

class TestETLOrchestrator(unittest.TestCase):

    def setUp(self):
        """Set up a mock loader and config for all tests."""
        self.mock_loader = MagicMock()
        # The factory will now return our single mock loader instance
        self.mock_loader_factory = MagicMock(return_value=self.mock_loader)

        self.mock_config = {
            'execution': {
                'max_workers': 1 # Run sequentially for predictable testing
            },
            'database': {
                'backend': 'postgres'
            },
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

    @patch('py_load_opentargets.orchestrator.get_checksum_manifest')
    @patch('py_load_opentargets.orchestrator.os.getenv')
    @patch('py_load_opentargets.orchestrator.download_dataset')
    def test_run_happy_path(self, mock_download, mock_getenv, mock_get_checksums):
        """Test a successful run for a single dataset."""
        # Arrange
        mock_getenv.return_value = "fake_db_conn_str"
        mock_download.return_value = Path("/fake/temp/dir/targets")
        mock_get_checksums.return_value = {'some_file': 'some_hash'}
        self.mock_loader.get_last_successful_version.return_value = None
        self.mock_loader.table_exists.return_value = True

        # Add checksum URI to config for this test
        self.mock_config['source']['checksum_uri_template'] = 'ftp://fake/{version}/'

        orchestrator = ETLOrchestrator(
            config=self.mock_config,
            datasets_to_process=['targets'],
            version=self.version,
            staging_schema=self.staging_schema,
            final_schema=self.final_schema
        )
        orchestrator.loader_factory = self.mock_loader_factory

        # Act
        orchestrator.run()

        # Assert
        mock_get_checksums.assert_called_once_with(self.version, 'ftp://fake/{version}/')
        mock_download.assert_called_once_with(
            'gcs://fake-bucket/{version}/{dataset_name}/',
            '22.04',
            'targets',
            ANY,  # The temp path is unpredictable
            {'some_file': 'some_hash'}, # The checksum manifest
            max_workers=1
        )
        self.mock_loader.connect.assert_called_once_with("fake_db_conn_str")
        self.mock_loader.prepare_staging_table.assert_called_once()
        self.mock_loader.execute_merge_strategy.assert_called_once()
        self.mock_loader.update_metadata.assert_called_once_with(
            version=self.version, dataset='targets', success=True, row_count=ANY
        )
        self.mock_loader.cleanup.assert_called_once()

    @patch('py_load_opentargets.orchestrator.get_checksum_manifest')
    @patch('py_load_opentargets.orchestrator.os.getenv')
    @patch('py_load_opentargets.orchestrator.download_dataset')
    def test_run_error_handling(self, mock_download, mock_getenv, mock_get_checksums):
        """Test that an error during bulk load is handled correctly."""
        # Arrange
        mock_getenv.return_value = "fake_db_conn_str"
        mock_download.return_value = Path("/fake/temp/dir/diseases")
        mock_get_checksums.return_value = {}
        self.mock_loader.get_last_successful_version.return_value = None
        self.mock_loader.bulk_load_native.side_effect = Exception("DB connection lost")

        orchestrator = ETLOrchestrator(
            config=self.mock_config,
            datasets_to_process=['diseases'],
            version=self.version,
            staging_schema=self.staging_schema,
            final_schema=self.final_schema,
            continue_on_error=True
        )
        orchestrator.loader_factory = self.mock_loader_factory

        # Act
        orchestrator.run()

        # Assert
        mock_download.assert_called_once_with(
            ANY, ANY, 'diseases', ANY, {}, max_workers=ANY
        )
        self.mock_loader.execute_merge_strategy.assert_not_called()
        self.mock_loader.update_metadata.assert_called_once_with(
            version=self.version, dataset='diseases', success=False, row_count=0, error_message="DB connection lost"
        )
        self.mock_loader.cleanup.assert_called_once()

    @patch('py_load_opentargets.orchestrator.os.getenv')
    def test_run_skips_already_loaded_version(self, mock_getenv):
        """Test that the orchestrator skips a dataset if the same version is already loaded."""
        # Arrange
        mock_getenv.return_value = "fake_db_conn_str"
        self.mock_loader.get_last_successful_version.return_value = self.version

        orchestrator = ETLOrchestrator(
            config=self.mock_config,
            datasets_to_process=['targets'],
            version=self.version,
            staging_schema=self.staging_schema,
            final_schema=self.final_schema,
            skip_confirmation=False
        )
        orchestrator.loader_factory = self.mock_loader_factory

        # Act
        orchestrator.run()

        # Assert
        self.mock_loader.connect.assert_called_once_with("fake_db_conn_str")
        self.mock_loader.prepare_staging_table.assert_not_called()
        self.mock_loader.bulk_load_native.assert_not_called()
        self.mock_loader.update_metadata.assert_not_called()
        self.mock_loader.cleanup.assert_called_once()

if __name__ == '__main__':
    unittest.main()
