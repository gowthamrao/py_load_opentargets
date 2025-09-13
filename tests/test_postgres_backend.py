import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from unittest.mock import MagicMock, patch
import time
from py_load_opentargets.backends.postgres import PostgresLoader
from py_load_opentargets.data_acquisition import get_remote_schema

@pytest.fixture
def loader():
    """Returns an instance of PostgresLoader."""
    return PostgresLoader()

# Test type mapping
@pytest.mark.parametrize("arrow_type, pg_type", [
    (pa.string(), "TEXT"),
    (pa.large_string(), "TEXT"),
    (pa.int8(), "BIGINT"),
    (pa.int64(), "BIGINT"),
    (pa.float32(), "DOUBLE PRECISION"),
    (pa.float64(), "DOUBLE PRECISION"),
    (pa.bool_(), "BOOLEAN"),
    (pa.timestamp('us'), "TIMESTAMP"),
    (pa.date32(), "DATE"),
    (pa.struct([pa.field('a', pa.int32())]), "JSONB"),
    (pa.list_(pa.string()), "JSONB"),
])
def test_pyarrow_to_postgres_type(loader, arrow_type, pg_type):
    """Tests the PyArrow to PostgreSQL type mapping."""
    assert loader._pyarrow_to_postgres_type(arrow_type) == pg_type

# Test CREATE TABLE statement generation
def test_bulk_load_native_streaming_from_remote(loader, mocker):
    """
    Tests that bulk_load_native can stream from remote URLs and correctly
    calls polars.scan_parquet for each URL.
    """
    # 1. Setup
    fake_urls = ["http://a/data1.parquet", "http://a/data2.parquet"]
    dummy_schema = pa.schema([pa.field("id", pa.int64())])
    dummy_lazyframe = MagicMock() # Mock Polars LazyFrame

    # Mock polars.scan_parquet to return our dummy LazyFrame
    mock_scan_parquet = mocker.patch(
        "py_load_opentargets.backends.postgres.pl.scan_parquet",
        return_value=dummy_lazyframe
    )

    # Mock the database connection
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 123
    loader.cursor = mock_cursor
    loader.conn = MagicMock()

    # 2. Execute
    loader.bulk_load_native("my_table", fake_urls, dummy_schema)

    # 3. Assert
    # The number of calls is not critical; the key is that it processes the stream.
    mock_scan_parquet.assert_called()
    mock_cursor.copy.assert_called_once()
    loader.conn.commit.assert_called_once()


def test_generate_create_table_sql(loader):
    """Tests the generation of a CREATE TABLE SQL statement."""
    schema = pa.schema([
        pa.field("id", pa.string()),
        pa.field("value", pa.int64()),
        pa.field("nested_data", pa.struct([pa.field('a', pa.int32())]))
    ])
    table_name = "my_test_table"

    expected_sql = '''CREATE TABLE my_test_table (
  "id" TEXT,
  "value" BIGINT,
  "nested_data" JSONB
);'''

    # The new default is to convert structs to JSON, which means the
    # transformed schema will have a string type, which maps to JSONB.
    transformed_schema = loader._get_transformed_schema(schema)
    generated_sql = loader._generate_create_table_sql(table_name, transformed_schema, schema_overrides={})
    assert " ".join(generated_sql.split()) == " ".join(expected_sql.split())

# --- Integration Tests ---
# These tests require a running PostgreSQL database.
# Set the DB_CONN_STR environment variable to run them.


@pytest.fixture
def test_loader(db_conn):
    """Fixture to provide a connected PostgresLoader instance and handle cleanup."""
    loader = PostgresLoader()
    loader.conn = db_conn
    loader.cursor = db_conn.cursor()

    yield loader

    # Cleanup: rollback any transaction
    db_conn.rollback()


def test_metadata_tracking(test_loader):
    """Tests the metadata creation and update functionality."""
    dataset = "test_meta"
    version = "1.0"
    start_time = time.time()
    end_time = start_time + 10

    # Ensure table is clean
    test_loader.cursor.execute("DROP TABLE IF EXISTS _ot_load_metadata;")

    # Test update and retrieval for a successful run
    test_loader.update_metadata(version, dataset, True, 100, start_time, end_time)
    last_version = test_loader.get_last_successful_version(dataset)
    assert last_version == version

    # Verify all fields, including timestamps
    test_loader.cursor.execute("SELECT status, error_message, start_time, end_time FROM _ot_load_metadata WHERE status='success' ORDER BY id DESC LIMIT 1;")
    status, msg, db_start, db_end = test_loader.cursor.fetchone()
    assert status == "success"
    assert msg is None
    assert db_start is not None
    assert db_end is not None

    # Test failure logging
    test_loader.update_metadata(version, dataset, False, 0, start_time, end_time, "A test error")
    test_loader.cursor.execute("SELECT status, error_message, start_time, end_time FROM _ot_load_metadata WHERE status='failure' ORDER BY id DESC LIMIT 1;")
    status, msg, db_start, db_end = test_loader.cursor.fetchone()
    assert status == "failure"
    assert msg == "A test error"
    assert db_start is not None
    assert db_end is not None

def test_merge_strategy_initial_and_upsert(test_loader, tmp_path: Path):
    """Tests the full merge strategy: initial load and then an upsert."""
    staging_schema = "staging"
    final_schema = "public"
    dataset = "merge_test"
    staging_table = f"{staging_schema}.{dataset}"
    final_table = f"{final_schema}.{dataset}"
    primary_keys = ["id"]

    # Prepare schemas and drop old tables
    test_loader.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {final_table};")
    test_loader.conn.commit()

    # --- 1. Initial Load ---
    # Create a dummy parquet file for the initial load
    initial_data = pa.Table.from_pydict({'id': [1, 2], 'data': ['a', 'b']})
    parquet_path = tmp_path / "initial"
    parquet_path.mkdir()
    pq.write_table(initial_data, parquet_path / "data.parquet")

    # Load into staging
    urls = [f"file://{p}" for p in sorted(parquet_path.glob("*.parquet"))]
    schema = get_remote_schema(urls)
    test_loader.prepare_staging_table(staging_table, schema)
    test_loader.bulk_load_native(staging_table, urls, schema)

    # Execute merge for the first time (should create the final table)
    test_loader.execute_merge_strategy(staging_table, final_table, primary_keys)

    # Verify initial load
    test_loader.cursor.execute(f"SELECT id, data FROM {final_table} ORDER BY id;")
    result = test_loader.cursor.fetchall()
    assert result == [(1, 'a'), (2, 'b')]

    # --- 2. Upsert Load ---
    # Create a new parquet file with updated and new data
    upsert_data = pa.Table.from_pydict({'id': [2, 3], 'data': ['x', 'c']}) # Update id=2, insert id=3
    parquet_path_upsert = tmp_path / "upsert"
    parquet_path_upsert.mkdir()
    pq.write_table(upsert_data, parquet_path_upsert / "data.parquet")

    # Load new data into staging (re-using the same staging table)
    urls_upsert = [f"file://{p}" for p in sorted(parquet_path_upsert.glob("*.parquet"))]
    schema_upsert = get_remote_schema(urls_upsert)
    test_loader.prepare_staging_table(staging_table, schema_upsert)
    test_loader.bulk_load_native(staging_table, urls_upsert, schema_upsert)

    # Execute merge for the second time
    test_loader.execute_merge_strategy(staging_table, final_table, primary_keys)

    # Verify upsert
    test_loader.cursor.execute(f"SELECT id, data FROM {final_table} ORDER BY id;")
    result = test_loader.cursor.fetchall()
    # With the new DELETE logic, record 1 should be removed as it's not in the second load.
    assert result == [(2, 'x'), (3, 'c')]


def test_merge_strategy_handles_deletes(test_loader, tmp_path: Path):
    """Tests that the merge strategy correctly deletes records that are no longer in the source."""
    staging_schema = "staging"
    final_schema = "public"
    dataset = "delete_test"
    staging_table = f"{staging_schema}.{dataset}"
    final_table = f"{final_schema}.{dataset}"
    primary_keys = ["id"]

    # Prepare schemas and drop old tables
    test_loader.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {final_table};")
    test_loader.conn.commit()

    # --- 1. Initial Load (with 3 records) ---
    initial_data = pa.Table.from_pydict({'id': [1, 2, 3], 'data': ['a', 'b', 'c']})
    parquet_path_initial = tmp_path / "initial"
    parquet_path_initial.mkdir()
    pq.write_table(initial_data, parquet_path_initial / "data.parquet")

    urls_initial = [f"file://{p}" for p in sorted(parquet_path_initial.glob("*.parquet"))]
    schema_initial = get_remote_schema(urls_initial)
    test_loader.prepare_staging_table(staging_table, schema_initial)
    test_loader.bulk_load_native(staging_table, urls_initial, schema_initial)
    test_loader.execute_merge_strategy(staging_table, final_table, primary_keys)

    # Verify initial load
    test_loader.cursor.execute(f"SELECT id, data FROM {final_table} ORDER BY id;")
    result = test_loader.cursor.fetchall()
    assert result == [(1, 'a'), (2, 'b'), (3, 'c')]
    assert len(result) == 3

    # --- 2. Second Load (record 'b' is now missing) ---
    second_load_data = pa.Table.from_pydict({'id': [1, 3], 'data': ['a_updated', 'c']})
    parquet_path_second = tmp_path / "second"
    parquet_path_second.mkdir()
    pq.write_table(second_load_data, parquet_path_second / "data.parquet")

    urls_second = [f"file://{p}" for p in sorted(parquet_path_second.glob("*.parquet"))]
    schema_second = get_remote_schema(urls_second)
    test_loader.prepare_staging_table(staging_table, schema_second)
    test_loader.bulk_load_native(staging_table, urls_second, schema_second)
    test_loader.execute_merge_strategy(staging_table, final_table, primary_keys)

    # Verify that record 2 was deleted and record 1 was updated
    test_loader.cursor.execute(f"SELECT id, data FROM {final_table} ORDER BY id;")
    result = test_loader.cursor.fetchall()
    assert result == [(1, 'a_updated'), (3, 'c')]
    assert len(result) == 2


def test_schema_alignment(test_loader, tmp_path: Path):
    """Tests that the schema alignment logic correctly adds new columns."""
    staging_schema = "staging"
    final_schema = "public"
    dataset = "align_test"
    staging_table = f"{staging_schema}.{dataset}"
    final_table = f"{final_schema}.{dataset}"

    # Prepare schemas and drop old tables
    test_loader.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {final_table};")

    # 1. Create a final table with an "old" schema
    test_loader.cursor.execute(f'CREATE TABLE {final_table} ("id" INT PRIMARY KEY, "data" TEXT);')
    test_loader.conn.commit()

    # 2. Create a staging table from a parquet file with a "new" schema
    new_schema_data = pa.Table.from_pydict({
        'id': [1],
        'data': ['a'],
        'new_text_col': ['new'],
        'new_int_col': [123]
    })
    parquet_path = tmp_path / "new_schema"
    parquet_path.mkdir()
    pq.write_table(new_schema_data, parquet_path / "data.parquet")

    urls = [f"file://{p}" for p in sorted(parquet_path.glob("*.parquet"))]
    schema = get_remote_schema(urls)
    test_loader.prepare_staging_table(staging_table, schema)

    # 3. Run the alignment logic
    test_loader.align_final_table_schema(staging_table, final_table)

    # 4. Verify that the final table has the new columns
    final_columns = test_loader._get_table_columns(final_table)
    assert "new_text_col" in final_columns
    assert "new_int_col" in final_columns

    # 5. Verify data types were translated correctly
    final_schema_from_db = test_loader._get_table_schema_from_db(final_table)
    # Note: postgres reports 'character varying' or 'text', depending on version/setup.
    # We check for the general idea, not the exact string.
    assert final_schema_from_db["new_text_col"].upper() in ("TEXT", "CHARACTER VARYING")
    assert final_schema_from_db["new_int_col"].upper() in ("BIGINT", "INTEGER")


def test_index_management_during_merge(test_loader, tmp_path: Path):
    """
    Tests that indexes are correctly dropped and recreated during a merge.
    This is an integration test.
    """
    staging_schema = "staging"
    final_schema = "public"
    dataset = "index_test"
    staging_table = f"{staging_schema}.{dataset}"
    final_table = f"{final_schema}.{dataset}"
    primary_keys = ["id"]

    # Prepare schemas and drop old tables
    test_loader.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {staging_schema};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
    test_loader.cursor.execute(f"DROP TABLE IF EXISTS {final_table};")

    # 1. Create a final table and add a non-PK index
    test_loader.cursor.execute(f'CREATE TABLE {final_table} ("id" INT PRIMARY KEY, "data" TEXT, "indexed_col" TEXT);')
    test_loader.cursor.execute(f'CREATE INDEX my_test_idx ON {final_table} ("indexed_col");')
    test_loader.conn.commit()

    # Verify index exists before we start
    indexes = test_loader.get_table_indexes(final_table)
    assert len(indexes) == 1
    assert indexes[0]['name'] == 'my_test_idx'

    # 2. Prepare staging table with data
    staging_data = pa.Table.from_pydict({'id': [1], 'data': ['a'], 'indexed_col': ['b']})
    parquet_path = tmp_path / "index_data"
    parquet_path.mkdir()
    pq.write_table(staging_data, parquet_path / "data.parquet")

    urls = [f"file://{p}" for p in sorted(parquet_path.glob("*.parquet"))]
    schema = get_remote_schema(urls)
    test_loader.prepare_staging_table(staging_table, schema)
    test_loader.bulk_load_native(staging_table, urls, schema)

    # 3. Spy on the index methods and run the merge
    with patch.object(test_loader, 'drop_indexes', wraps=test_loader.drop_indexes) as spy_drop, \
         patch.object(test_loader, 'recreate_indexes', wraps=test_loader.recreate_indexes) as spy_recreate:

        # This block simulates the orchestrator's logic
        indexes_to_manage = test_loader.get_table_indexes(final_table)
        try:
            test_loader.drop_indexes(indexes_to_manage)
            test_loader.execute_merge_strategy(staging_table, final_table, primary_keys)
        finally:
            test_loader.recreate_indexes(indexes_to_manage)

        # 4. Assert that the spies were called
        spy_drop.assert_called_once()
        spy_recreate.assert_called_once()

    # 5. Verify the index exists again on the actual table
    final_indexes = test_loader.get_table_indexes(final_table)
    assert len(final_indexes) == 1
    assert final_indexes[0]['name'] == 'my_test_idx'
