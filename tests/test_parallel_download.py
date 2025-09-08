import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path

from py_load_opentargets.orchestrator import ETLOrchestrator

# A dummy config that defines two datasets
TEST_CONFIG = {
    'source': {
        'version_discovery_uri': 'memory://mock_fs/',
        'data_download_uri_template': 'memory://mock_fs/{version}/{dataset_name}'
    },
    'datasets': {
        'targets': {
            'primary_key': ['id']
        },
        'diseases': {
            'primary_key': ['id']
        }
    }
}

@pytest.fixture
def mock_fs():
    """Fixture to create a mock fsspec memory filesystem with a smarter patch."""
    from fsspec.implementations.memory import MemoryFileSystem
    fs = MemoryFileSystem()

    # Create dummy release and dataset files at paths that match the URLs
    base_path = "mock_fs/23.09"
    fs.mkdir(f"{base_path}/targets")
    fs.touch(f"{base_path}/targets/part-0.parquet")
    fs.mkdir(f"{base_path}/diseases")
    fs.touch(f"{base_path}/diseases/part-0.parquet")

    def url_to_fs_side_effect(url, **kwargs):
        """A side effect that correctly parses the path from the memory URI."""
        path = url.replace("memory://", "")
        return fs, path

    # Patch fsspec to use our side effect, which returns the mock fs
    # and the correct path from the URL.
    with patch('fsspec.core.url_to_fs', side_effect=url_to_fs_side_effect):
        yield fs

def test_orchestrator_runs_downloads_in_parallel(mock_fs):
    """
    Verify that the ETLOrchestrator attempts to download multiple datasets
    and then proceeds to load them.
    """
    # 1. Setup Mocks
    mock_loader = MagicMock()
    mock_loader.get_last_successful_version.return_value = None

    # We need to mock the internal loading function to prevent it from
    # actually trying to do database operations.
    with patch('py_load_opentargets.orchestrator.ETLOrchestrator._download_and_load_dataset') as mock_load_method:

        # 2. Initialize Orchestrator
        orchestrator = ETLOrchestrator(
            config=TEST_CONFIG,
            loader=mock_loader,
            datasets_to_process=['targets', 'diseases'],
            version='23.09',
            staging_schema='staging',
            final_schema='public',
            max_workers=2 # Use 2 workers for the test
        )

        # 3. Run the orchestrator
        orchestrator.run()

        # 4. Assertions
        # Assert that the pre-flight check was done for both datasets
        mock_loader.get_last_successful_version.assert_has_calls([
            call('targets'),
            call('diseases')
        ], any_order=True)

        # Assert that the loading method was called for both datasets
        # This proves that the download was considered successful for both
        assert mock_load_method.call_count == 2

        # Check that the calls were made with the correct dataset names and a Path object
        # The exact Path object is tricky to assert, so we check the type
        call_args_list = mock_load_method.call_args_list
        called_datasets = {args[0][0] for args in call_args_list}

        assert called_datasets == {'targets', 'diseases'}
        assert isinstance(call_args_list[0].args[1], Path)
        assert 'targets' in str(call_args_list[0].args[1]) or 'diseases' in str(call_args_list[0].args[1])
        assert isinstance(call_args_list[1].args[1], Path)
        assert 'targets' in str(call_args_list[1].args[1]) or 'diseases' in str(call_args_list[1].args[1])
