import pytest
import pyarrow as pa
from py_load_opentargets.backends.postgres import PostgresLoader

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

    # Normalize whitespace for comparison
    generated_sql = loader._generate_create_table_sql(table_name, schema)
    assert " ".join(generated_sql.split()) == " ".join(expected_sql.split())

# --- Integration Tests ---
# These tests require a running PostgreSQL database.
# Set the DB_CONN_STR environment variable to run them.
import os
import psycopg2
import pyarrow.parquet as pq
from pathlib import Path

DB_CONN_STR = os.environ.get("DB_CONN_STR")

@pytest.fixture(scope="module")
def db_conn():
    """Fixture to provide a database connection for integration tests."""
    if not DB_CONN_STR:
        pytest.skip("DB_CONN_STR environment variable not set. Skipping integration tests.")

    conn = psycopg2.connect(DB_CONN_STR)
    yield conn
    conn.close()

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

    # Ensure table is clean
    test_loader.cursor.execute("DROP TABLE IF EXISTS _ot_load_metadata;")

    # Test update and retrieval
    test_loader.update_metadata(version, dataset, True, 100)
    last_version = test_loader.get_last_successful_version(dataset)
    assert last_version == version

    # Test failure logging
    test_loader.update_metadata(version, dataset, False, 0, "A test error")
    test_loader.cursor.execute("SELECT status, error_message FROM _ot_load_metadata ORDER BY id DESC LIMIT 1;")
    status, msg = test_loader.cursor.fetchone()
    assert status == "failure"
    assert msg == "A test error"

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
    test_loader.prepare_staging_table(staging_table, parquet_path)
    test_loader.bulk_load_native(staging_table, parquet_path)

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
    test_loader.prepare_staging_table(staging_table, parquet_path_upsert)
    test_loader.bulk_load_native(staging_table, parquet_path_upsert)

    # Execute merge for the second time
    test_loader.execute_merge_strategy(staging_table, final_table, primary_keys)

    # Verify upsert
    test_loader.cursor.execute(f"SELECT id, data FROM {final_table} ORDER BY id;")
    result = test_loader.cursor.fetchall()
    assert result == [(1, 'a'), (2, 'x'), (3, 'c')]
