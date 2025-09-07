import pytest
import os
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from click.testing import CliRunner

from py_load_opentargets.cli import load_postgres
from py_load_opentargets.data_acquisition import list_available_versions, download_dataset

DB_CONN_STR = os.environ.get("DB_CONN_STR")

@pytest.fixture(scope="module")
def db_conn():
    """Fixture to provide a database connection for integration tests."""
    if not DB_CONN_STR:
        pytest.skip("DB_CONN_STR environment variable not set. Skipping integration tests.")

    conn = psycopg2.connect(DB_CONN_STR)
    yield conn
    conn.close()

@pytest.fixture(autouse=True)
def cleanup_db(db_conn):
    """Auto-cleanup fixture to drop tables and schemas after each test."""
    yield
    cursor = db_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS public.test_cli;")
    cursor.execute("DROP TABLE IF EXISTS staging.test_cli;")
    cursor.execute("DROP TABLE IF EXISTS _ot_load_metadata;")
    cursor.execute("DROP SCHEMA IF EXISTS staging;")
    db_conn.commit()

@pytest.fixture
def mock_data_acquisition(monkeypatch, tmp_path: Path):
    """Mocks the data acquisition functions to use local dummy data."""
    # Create dummy parquet file
    dummy_data = pa.Table.from_pydict({'id': [1, 2], 'data': ['a', 'b']})
    dataset_path = tmp_path / "25.01" / "test_cli"
    dataset_path.mkdir(parents=True)
    pq.write_table(dummy_data, dataset_path / "data.parquet")

    def mock_list_versions():
        return ["25.01", "25.00"]

    def mock_download(version, dataset, output_dir):
        # This function just needs to return the path to the dummy data
        return dataset_path

    monkeypatch.setattr("py_load_opentargets.cli.list_available_versions", mock_list_versions)
    monkeypatch.setattr("py_load_opentargets.cli.download_dataset", mock_download)


def test_cli_end_to_end_load(db_conn, mock_data_acquisition):
    """
    Tests the 'load-postgres' CLI command from end to end.
    Mocks the download step to use a local dummy parquet file.
    """
    runner = CliRunner()
    args = [
        "--db-conn-str", DB_CONN_STR,
        "--dataset", "test_cli",
        "--version", "25.01",
        "--primary-key", "id",
        "--final-table-name", "test_cli",
        "--skip-confirmation", # Avoid interactive prompts in tests
    ]

    result = runner.invoke(load_postgres, args)

    assert result.exit_code == 0, f"CLI command failed with output:\n{result.output}"
    assert "✅ Success! Merged 2 rows" in result.output

    # Verify final table content
    cursor = db_conn.cursor()
    cursor.execute("SELECT id, data FROM public.test_cli ORDER BY id;")
    final_data = cursor.fetchall()
    assert final_data == [(1, 'a'), (2, 'b')]

    # Verify metadata table content
    cursor.execute("SELECT opentargets_version, dataset_name, status, rows_loaded FROM _ot_load_metadata;")
    meta_data = cursor.fetchone()
    assert meta_data == ("25.01", "test_cli", "success", 2)
