import unittest
from unittest.mock import MagicMock, patch, ANY
import pyarrow as pa

from py_load_opentargets.orchestrator import ETLOrchestrator

class TestETLOrchestrator(unittest.TestCase):

    def setUp(self):
        """Set up a mock loader and config for all tests."""
        self.mock_loader = MagicMock()
        self.mock_loader_factory = MagicMock(return_value=self.mock_loader)

        self.mock_config = {
            'execution': {
                'max_workers': 1,
                'load_strategy': 'stream' # Default to new strategy
            },
            'database': {
                'backend': 'postgres'
            },
            'source': {
                'data_uri_template': 'http://fake/{version}/{dataset_name}',
                'data_download_uri_template': 'gcs://fake-bucket/{version}/{dataset_name}/',
                'checksum_uri_template': 'gcs://fake-checksums/{version}/'
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

    @patch('py_load_opentargets.orchestrator.get_remote_schema')
    @patch('py_load_opentargets.orchestrator.get_remote_dataset_urls')
    @patch('py_load_opentargets.orchestrator.get_checksum_manifest')
    @patch('py_load_opentargets.orchestrator.os.getenv')
    def test_run_happy_path_stream_strategy(self, mock_getenv, mock_get_checksums, mock_get_urls, mock_get_schema):
        """Test a successful run using the 'stream' strategy."""
        # Arrange
        mock_getenv.side_effect = lambda key: "fake_db_conn_str" if key == "DB_CONN_STR" else None
        mock_get_checksums.return_value = {}
        mock_get_urls.return_value = ["http://fake/file1.parquet"]
        mock_get_schema.return_value = pa.schema([pa.field("id", pa.int64())])
        self.mock_loader.get_last_successful_version.return_value = None
        self.mock_loader.table_exists.return_value = True

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
        mock_get_urls.assert_called_once_with(ANY, self.version, 'targets')
        mock_get_schema.assert_called_once_with(["http://fake/file1.parquet"])
        self.mock_loader.connect.assert_called_once_with("fake_db_conn_str", ANY)
        self.mock_loader.prepare_staging_table.assert_called_once_with(ANY, mock_get_schema.return_value)
        self.mock_loader.bulk_load_native.assert_called_once_with(ANY, ["http://fake/file1.parquet"], mock_get_schema.return_value)
        self.mock_loader.execute_merge_strategy.assert_called_once()
        self.mock_loader.update_metadata.assert_called_once_with(
            version=self.version, dataset='targets', success=True, row_count=ANY
        )
        self.mock_loader.cleanup.assert_called_once()

    @patch('py_load_opentargets.orchestrator.get_remote_schema')
    @patch('py_load_opentargets.orchestrator.get_remote_dataset_urls')
    @patch('py_load_opentargets.orchestrator.get_checksum_manifest')
    @patch('py_load_opentargets.orchestrator.os.getenv')
    def test_run_error_handling_stream_strategy(self, mock_getenv, mock_get_checksums, mock_get_urls, mock_get_schema):
        """Test that an error during bulk load is handled correctly with the stream strategy."""
        # Arrange
        mock_getenv.side_effect = lambda key: "fake_db_conn_str" if key == "DB_CONN_STR" else None
        mock_get_checksums.return_value = {}
        mock_get_urls.return_value = ["http://fake/file1.parquet"]
        mock_get_schema.return_value = pa.schema([pa.field("id", pa.int64())])
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
        mock_get_urls.assert_called_once()
        self.mock_loader.execute_merge_strategy.assert_not_called()
        self.mock_loader.update_metadata.assert_called_once_with(
            version=self.version, dataset='diseases', success=False, row_count=0, error_message="DB connection lost"
        )
        self.mock_loader.cleanup.assert_called_once()

    @patch('py_load_opentargets.orchestrator.get_checksum_manifest')
    @patch('py_load_opentargets.orchestrator.os.getenv')
    def test_run_skips_already_loaded_version(self, mock_getenv, mock_get_checksums):
        """Test that the orchestrator skips a dataset if the same version is already loaded."""
        # Arrange
        mock_getenv.side_effect = lambda key: "fake_db_conn_str" if key == "DB_CONN_STR" else None
        mock_get_checksums.return_value = {}
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
        self.mock_loader.connect.assert_called_once_with("fake_db_conn_str", ANY)
        self.mock_loader.prepare_staging_table.assert_not_called()
        self.mock_loader.bulk_load_native.assert_not_called()
        # It will still try to update metadata with a failure for the skip
        self.mock_loader.update_metadata.assert_not_called()
        self.mock_loader.cleanup.assert_called_once()

if __name__ == '__main__':
    unittest.main()
