import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from unittest.mock import patch

from py_load_opentargets.orchestrator import ETLOrchestrator

def test_full_end_to_end_run(db_conn, db_conn_str, tmp_path: Path):
    """
    Tests a full, end-to-end run of the ETLOrchestrator using a live,
    containerized Postgres database and mocked data acquisition.
    """
    # 1. Setup: Create dummy Parquet files
    source_path = tmp_path / "source"
    source_path.mkdir()
    targets_path = source_path / "targets"
    targets_path.mkdir()
    diseases_path = source_path / "diseases"
    diseases_path.mkdir()

    targets_data = pa.Table.from_pydict({'id': ['target1', 'target2'], 'value': [100, 200]})
    pq.write_table(targets_data, targets_path / "data.parquet")

    diseases_data = pa.Table.from_pydict({'id': ['diseaseA', 'diseaseB'], 'description': ['descA', 'descB']})
    pq.write_table(diseases_data, diseases_path / "data.parquet")

    # 2. Setup: Mock config and data acquisition
    mock_config = {
        'execution': {'max_workers': 1, 'load_strategy': 'stream'},
        'database': {'backend': 'postgres'},
        'source': {
            'provider': 'gcs_ftp',
            'gcs_ftp': {
                'data_uri_template': str(source_path) + '/{dataset_name}',
                'data_download_uri_template': str(source_path) + '/{dataset_name}/',
            'checksum_uri_template': 'dummy_path/{version}'
            }
        },
        'datasets': {
            'targets': {'primary_key': ['id']},
            'diseases': {'primary_key': ['id']}
        }
    }

    targets_urls = [f"file://{p}" for p in sorted(targets_path.glob("*.parquet"))]
    diseases_urls = [f"file://{p}" for p in sorted(diseases_path.glob("*.parquet"))]

    # 3. Run the Orchestrator
    # We patch the data acquisition functions and os.getenv to avoid network calls and set the DB string
    with patch('py_load_opentargets.orchestrator.get_remote_dataset_urls') as mock_get_urls, \
         patch('py_load_opentargets.orchestrator.get_checksum_manifest') as mock_get_checksums, \
         patch('py_load_opentargets.orchestrator.verify_remote_dataset') as mock_verify, \
         patch('os.getenv') as mock_getenv:

        # Configure mocks
        mock_get_urls.side_effect = lambda _, __, dataset_name: {
            'targets': targets_urls,
            'diseases': diseases_urls
        }[dataset_name]
        mock_get_checksums.return_value = {}  # Not testing checksums here
        mock_verify.return_value = (True, "")
        mock_getenv.return_value = db_conn_str

        # Instantiate and run orchestrator
        orchestrator = ETLOrchestrator(
            config=mock_config,
            datasets_to_process=['targets', 'diseases'],
            version="22.04",
            staging_schema="staging",
            final_schema="public",
            skip_confirmation=True  # Auto-approve CLI prompts
        )
        orchestrator.run()

    # 4. Assertions
    try:
        cursor = db_conn.cursor()

        # Check targets table
        cursor.execute('SELECT "id", "value" FROM public.targets ORDER BY "id";')
        targets_result = cursor.fetchall()
        assert targets_result == [('target1', 100), ('target2', 200)]

        # Check diseases table
        cursor.execute('SELECT "id", "description" FROM public.diseases ORDER BY "id";')
        diseases_result = cursor.fetchall()
        assert diseases_result == [('diseaseA', 'descA'), ('diseaseB', 'descB')]

        # Check metadata table
        cursor.execute("SELECT dataset_name, status FROM _ot_load_metadata WHERE opentargets_version='22.04' ORDER BY dataset_name;")
        meta_result = cursor.fetchall()
        assert meta_result == [('diseases', 'success'), ('targets', 'success')]
    finally:
        # 5. Cleanup
        with db_conn.cursor() as cursor:
            # Truncate metadata to avoid affecting other tests, and drop test-specific tables.
            cursor.execute("TRUNCATE _ot_load_metadata;")
            cursor.execute("DROP TABLE IF EXISTS public.targets, public.diseases, staging.targets, staging.diseases;")
            db_conn.commit()
