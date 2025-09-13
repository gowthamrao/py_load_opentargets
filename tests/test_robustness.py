import pytest
import pyarrow as pa
import pyarrow.parquet as pq
import polars
import psycopg
from pathlib import Path
from unittest.mock import patch

from py_load_opentargets.orchestrator import ETLOrchestrator


@pytest.mark.integration
def test_ftp_data_download_and_load(db_conn, db_conn_str):
    """
    Tests a full, end-to-end run of the ETLOrchestrator using a live,
    containerized Postgres database and a real FTP download.
    """
    # 1. Setup: Mock config to use FTP
    mock_config = {
        "execution": {"max_workers": 1, "load_strategy": "download"},
        "database": {"backend": "postgres"},
        "source": {
            "provider": "gcs_ftp",
            "gcs_ftp": {
                "ftp_host": "ftp.ebi.ac.uk",
                "ftp_path": "/pub/databases/opentargets/platform/{version}/output/etl/parquet/{dataset_name}",
                "data_uri_template": "ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform/{version}/output/etl/parquet/{dataset_name}",
                "data_download_uri_template": "ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform/{version}/output/etl/parquet/{dataset_name}/",
                "checksum_uri_template": "gs://open-targets-data-releases/{version}/checksums.txt",
            },
        },
        "datasets": {"drugwarnings": {"primary_key": ["id"]}},
    }

    # 2. Run the Orchestrator
    with (
        patch("os.getenv") as mock_getenv,
        patch("py_load_opentargets.orchestrator.download_dataset") as mock_download,
        patch(
            "py_load_opentargets.orchestrator.get_checksum_manifest"
        ) as mock_get_checksums,
    ):
        mock_getenv.return_value = db_conn_str
        mock_get_checksums.return_value = {}
        # Point the mock to our local test file
        mock_download.return_value = Path(__file__).parent / "resources"

        orchestrator = ETLOrchestrator(
            config=mock_config,
            datasets_to_process=["drugwarnings"],
            version="22.04",
            staging_schema="staging",
            final_schema="public",
            skip_confirmation=True,
        )
        orchestrator.run()

    # 3. Assertions
    try:
        cursor = db_conn.cursor()

        # Check drugwarnings table
        cursor.execute("SELECT COUNT(*) FROM public.drugwarnings;")
        count = cursor.fetchone()[0]
        assert count > 0

        # Check metadata table
        cursor.execute(
            "SELECT dataset_name, status FROM _ot_load_metadata WHERE opentargets_version='22.04' AND dataset_name='drugwarnings';"
        )
        meta_result = cursor.fetchone()
        assert meta_result == ("drugwarnings", "success")
    finally:
        # 4. Cleanup
        with db_conn.cursor() as cursor:
            cursor.execute("TRUNCATE _ot_load_metadata;")
            cursor.execute(
                "DROP TABLE IF EXISTS public.drugwarnings, staging.drugwarnings;"
            )
            db_conn.commit()


def test_empty_parquet_file(db_conn, db_conn_str, tmp_path: Path):
    """
    Tests that the loader can handle an empty Parquet file gracefully.
    """
    # 1. Setup: Create an empty Parquet file
    source_path = tmp_path / "source"
    source_path.mkdir()
    empty_path = source_path / "empty_dataset"
    empty_path.mkdir()

    # Create a schema but no data
    schema = pa.schema([pa.field("id", pa.string()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict({"id": [], "value": []}, schema=schema)
    pq.write_table(table, empty_path / "data.parquet")

    # 2. Setup: Mock config and data acquisition
    mock_config = {
        "execution": {"max_workers": 1, "load_strategy": "stream"},
        "database": {"backend": "postgres"},
        "source": {
            "provider": "gcs_ftp",
            "gcs_ftp": {
                "data_uri_template": str(source_path) + "/{dataset_name}",
                "data_download_uri_template": str(source_path) + "/{dataset_name}/",
                "checksum_uri_template": "dummy_path/{version}",
            },
        },
        "datasets": {"empty_dataset": {"primary_key": ["id"]}},
    }

    urls = [f"file://{p}" for p in sorted(empty_path.glob("*.parquet"))]

    # 3. Run the Orchestrator
    with (
        patch(
            "py_load_opentargets.orchestrator.get_remote_dataset_urls"
        ) as mock_get_urls,
        patch(
            "py_load_opentargets.orchestrator.get_checksum_manifest"
        ) as mock_get_checksums,
        patch("py_load_opentargets.orchestrator.verify_remote_dataset") as mock_verify,
        patch("os.getenv") as mock_getenv,
    ):
        mock_get_urls.return_value = urls
        mock_get_checksums.return_value = {}
        mock_verify.return_value = (True, "")
        mock_getenv.return_value = db_conn_str

        orchestrator = ETLOrchestrator(
            config=mock_config,
            datasets_to_process=["empty_dataset"],
            version="22.04",
            staging_schema="staging",
            final_schema="public",
            skip_confirmation=True,
        )
        orchestrator.run()

    # 4. Assertions
    try:
        cursor = db_conn.cursor()

        # Check that the table was created but is empty
        cursor.execute("SELECT COUNT(*) FROM public.empty_dataset;")
        count = cursor.fetchone()[0]
        assert count == 0

        # Check metadata table
        cursor.execute(
            "SELECT dataset_name, status FROM _ot_load_metadata WHERE opentargets_version='22.04' AND dataset_name='empty_dataset';"
        )
        meta_result = cursor.fetchone()
        assert meta_result == ("empty_dataset", "success")
    finally:
        # 5. Cleanup
        with db_conn.cursor() as cursor:
            cursor.execute("TRUNCATE _ot_load_metadata;")
            cursor.execute(
                "DROP TABLE IF EXISTS public.empty_dataset, staging.empty_dataset;"
            )
            db_conn.commit()


def test_schema_mismatch(db_conn, db_conn_str, tmp_path: Path):
    """
    Tests that the loader fails gracefully when Parquet files have different schemas.
    """
    # 1. Setup: Create Parquet files with different schemas
    source_path = tmp_path / "source"
    source_path.mkdir()
    mismatch_path = source_path / "mismatch_dataset"
    mismatch_path.mkdir()

    schema1 = pa.schema([pa.field("id", pa.string()), pa.field("value", pa.int64())])
    table1 = pa.Table.from_pydict({"id": ["a"], "value": [1]}, schema=schema1)
    pq.write_table(table1, mismatch_path / "data1.parquet")

    schema2 = pa.schema([pa.field("id", pa.string()), pa.field("value2", pa.int64())])
    table2 = pa.Table.from_pydict({"id": ["b"], "value2": [2]}, schema=schema2)
    pq.write_table(table2, mismatch_path / "data2.parquet")

    # 2. Setup: Mock config and data acquisition
    mock_config = {
        "execution": {"max_workers": 1, "load_strategy": "stream"},
        "database": {"backend": "postgres"},
        "source": {
            "provider": "gcs_ftp",
            "gcs_ftp": {
                "data_uri_template": str(source_path) + "/{dataset_name}",
                "data_download_uri_template": str(source_path) + "/{dataset_name}/",
                "checksum_uri_template": "dummy_path/{version}",
            },
        },
        "datasets": {"mismatch_dataset": {"primary_key": ["id"]}},
    }

    urls = [f"file://{p}" for p in sorted(mismatch_path.glob("*.parquet"))]

    # 3. Run the Orchestrator and expect an exception
    with (
        pytest.raises(polars.exceptions.ColumnNotFoundError),
        patch(
            "py_load_opentargets.orchestrator.get_remote_dataset_urls"
        ) as mock_get_urls,
        patch(
            "py_load_opentargets.orchestrator.get_checksum_manifest"
        ) as mock_get_checksums,
        patch("py_load_opentargets.orchestrator.verify_remote_dataset") as mock_verify,
        patch("os.getenv") as mock_getenv,
    ):
        mock_get_urls.return_value = urls
        mock_get_checksums.return_value = {}
        mock_verify.return_value = (True, "")
        mock_getenv.return_value = db_conn_str

        orchestrator = ETLOrchestrator(
            config=mock_config,
            datasets_to_process=["mismatch_dataset"],
            version="22.04",
            staging_schema="staging",
            final_schema="public",
            skip_confirmation=True,
            continue_on_error=False,  # We want the exception to be raised
        )
        orchestrator.run()


def test_data_integrity_error(db_conn, db_conn_str, tmp_path: Path):
    """
    Tests that the loader fails gracefully when data violates database constraints.
    """
    # 1. Setup: Create a Parquet file with a NULL primary key
    source_path = tmp_path / "source"
    source_path.mkdir()
    integrity_path = source_path / "integrity_dataset"
    integrity_path.mkdir()

    schema = pa.schema([pa.field("id", pa.string()), pa.field("value", pa.int64())])
    table = pa.Table.from_pydict({"id": [None], "value": [1]}, schema=schema)
    pq.write_table(table, integrity_path / "data.parquet")

    # 2. Setup: Mock config and data acquisition
    mock_config = {
        "execution": {"max_workers": 1, "load_strategy": "stream"},
        "database": {"backend": "postgres"},
        "source": {
            "provider": "gcs_ftp",
            "gcs_ftp": {
                "data_uri_template": str(source_path) + "/{dataset_name}",
                "data_download_uri_template": str(source_path) + "/{dataset_name}/",
                "checksum_uri_template": "dummy_path/{version}",
            },
        },
        "datasets": {"integrity_dataset": {"primary_key": ["id"]}},
    }

    urls = [f"file://{p}" for p in sorted(integrity_path.glob("*.parquet"))]

    # 3. Run the Orchestrator
    with (
        pytest.raises(psycopg.errors.NotNullViolation),
        patch(
            "py_load_opentargets.orchestrator.get_remote_dataset_urls"
        ) as mock_get_urls,
        patch(
            "py_load_opentargets.orchestrator.get_checksum_manifest"
        ) as mock_get_checksums,
        patch("py_load_opentargets.orchestrator.verify_remote_dataset") as mock_verify,
        patch("os.getenv") as mock_getenv,
    ):
        mock_get_urls.return_value = urls
        mock_get_checksums.return_value = {}
        mock_verify.return_value = (True, "")
        mock_getenv.return_value = db_conn_str

        orchestrator = ETLOrchestrator(
            config=mock_config,
            datasets_to_process=["integrity_dataset"],
            version="22.04",
            staging_schema="staging",
            final_schema="public",
            skip_confirmation=True,
            continue_on_error=False,  # We want the exception to be raised
        )
        orchestrator.run()
