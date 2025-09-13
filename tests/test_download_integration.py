import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from unittest.mock import patch
import hashlib

from py_load_opentargets.orchestrator import ETLOrchestrator

def test_download_integration_run(db_conn, db_conn_str, tmp_path: Path):
    """
    Tests a full, end-to-end run of the ETLOrchestrator using a live,
    containerized Postgres database and a local filesystem source with the 'download' strategy.
    """
    # 1. Setup: Create a local directory structure and dummy files
    source_path = tmp_path / "source"
    version_path = source_path / "22.04"
    targets_path = version_path / "targets"
    diseases_path = version_path / "diseases"
    targets_path.mkdir(parents=True)
    diseases_path.mkdir(parents=True)

    targets_data = pa.Table.from_pydict({'id': ['target1', 'target2'], 'value': [100, 200]})
    diseases_data = pa.Table.from_pydict({'id': ['diseaseA', 'diseaseB'], 'description': ['descA', 'descB']})

    targets_file = targets_path / "data.parquet"
    diseases_file = diseases_path / "data.parquet"
    pq.write_table(targets_data, targets_file)
    pq.write_table(diseases_data, diseases_file)

    # 2. Create checksum manifest
    targets_checksum = hashlib.sha1(targets_file.read_bytes()).hexdigest()
    diseases_checksum = hashlib.sha1(diseases_file.read_bytes()).hexdigest()

    manifest_content = (
        f"{targets_checksum}  output/etl/parquet/targets/data.parquet\n"
        f"{diseases_checksum}  output/etl/parquet/diseases/data.parquet\n"
    )
    manifest_path = version_path / "release_data_integrity"
    manifest_path.write_text(manifest_content)

    manifest_checksum = hashlib.sha1(manifest_content.encode('utf-8')).hexdigest()
    checksum_file_path = version_path / "release_data_integrity.sha1"
    checksum_file_path.write_text(f"{manifest_checksum}  release_data_integrity")

    # 3. Setup: Mock config
    mock_config = {
        'execution': {'max_workers': 1, 'load_strategy': 'download'},
        'database': {'backend': 'postgres'},
        'source': {
            'provider': 'local',
            'local': {
                'data_uri_template': f"file://{version_path}" + "/{dataset_name}",
                'data_download_uri_template': f"file://{version_path}" + "/{dataset_name}/",
                'checksum_uri_template': f"file://{version_path}"
            }
        },
        'datasets': {
            'targets': {'primary_key': ['id']},
            'diseases': {'primary_key': ['id']}
        }
    }

    # 4. Run the Orchestrator
    with patch('os.getenv') as mock_getenv:
        mock_getenv.return_value = db_conn_str

        orchestrator = ETLOrchestrator(
            config=mock_config,
            datasets_to_process=['targets', 'diseases'],
            version="22.04",
            staging_schema="staging",
            final_schema="public",
            skip_confirmation=True
        )
        orchestrator.run()

    # 5. Assertions
    try:
        cursor = db_conn.cursor()

        cursor.execute('SELECT "id", "value" FROM public.targets ORDER BY "id";')
        targets_result = cursor.fetchall()
        assert targets_result == [('target1', 100), ('target2', 200)]

        cursor.execute('SELECT "id", "description" FROM public.diseases ORDER BY "id";')
        diseases_result = cursor.fetchall()
        assert diseases_result == [('diseaseA', 'descA'), ('diseaseB', 'descB')]

        cursor.execute("SELECT dataset_name, status FROM _ot_load_metadata WHERE opentargets_version='22.04' ORDER BY dataset_name;")
        meta_result = cursor.fetchall()
        assert meta_result == [('diseases', 'success'), ('targets', 'success')]
    finally:
        # 6. Cleanup
        with db_conn.cursor() as cursor:
            cursor.execute("TRUNCATE _ot_load_metadata;")
            cursor.execute("DROP TABLE IF EXISTS public.targets, public.diseases, staging.targets, staging.diseases;")
            db_conn.commit()
