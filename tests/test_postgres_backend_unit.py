import pytest
import pyarrow as pa
from py_load_opentargets.backends.postgres import PostgresLoader

@pytest.fixture
def loader():
    """Returns an instance of the PostgresLoader for unit testing."""
    return PostgresLoader()

def test_pyarrow_to_postgres_type_mapping(loader):
    """Tests the type mapping from PyArrow to PostgreSQL."""
    assert loader._pyarrow_to_postgres_type(pa.string()) == "TEXT"
    assert loader._pyarrow_to_postgres_type(pa.int64()) == "BIGINT"
    assert loader._pyarrow_to_postgres_type(pa.float64()) == "DOUBLE PRECISION"
    assert loader._pyarrow_to_postgres_type(pa.bool_()) == "BOOLEAN"
    assert loader._pyarrow_to_postgres_type(pa.timestamp('us')) == "TIMESTAMP"
    assert loader._pyarrow_to_postgres_type(pa.date32()) == "DATE"
    assert loader._pyarrow_to_postgres_type(pa.struct([])) == "JSONB"
    assert loader._pyarrow_to_postgres_type(pa.list_(pa.string())) == "JSONB"

def test_get_transformed_schema_default_json_conversion(loader, mocker):
    """
    Tests that with no overrides, nested types are kept as nested types in the
    transformed schema, so they can be mapped to JSONB.
    """
    mocker.patch('psycopg.connect')
    original_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('nested_data', pa.struct([pa.field('a', pa.string())]))
    ])

    loader.connect("dummy_conn_str", dataset_config={})
    transformed_schema = loader._get_transformed_schema(original_schema)

    # With no overrides, the schema should be identical.
    assert transformed_schema == original_schema

def test_get_transformed_schema_with_overrides(loader, mocker):
    """
    Tests that schema_overrides for 'flatten' and 'rename' are correctly applied.
    """
    mocker.patch('psycopg.connect')
    original_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('data_to_flatten', pa.struct([
            pa.field('field_a', pa.string()),
            pa.field('field_b', pa.int32())
        ])),
        pa.field('other_nested', pa.struct([pa.field('c', pa.string())]))
    ])

    config = {
        "schema_overrides": {
            "id": {"rename": "renamed_id"},
            "data_to_flatten": {"action": "flatten"}
        },
        "flatten_separator": "__"
    }
    loader.connect("dummy_conn_str", dataset_config=config)

    transformed_schema = loader._get_transformed_schema(original_schema)

    # The 'other_nested' struct should be preserved in the schema, not converted to string.
    expected_schema = pa.schema([
        pa.field('renamed_id', pa.int64()),
        pa.field('data_to_flatten__field_a', pa.string()),
        pa.field('data_to_flatten__field_b', pa.int32()),
        pa.field('other_nested', pa.struct([pa.field('c', pa.string())]))
    ])

    assert transformed_schema == expected_schema

def test_generate_create_table_sql_simple(loader):
    """Tests generating a CREATE TABLE statement for a simple schema."""
    schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('description', pa.string())
    ])

    sql = loader._generate_create_table_sql("public.my_table", schema, schema_overrides={})

    expected_sql = """CREATE TABLE public.my_table (
  "id" BIGINT,
  "description" TEXT
);"""
    assert sql.strip() == expected_sql.strip()

def test_generate_create_table_sql_with_flattened_schema(loader):
    """
    Tests generating a CREATE TABLE statement for a schema that has already
    been transformed (flattened).
    """
    flattened_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('location_chromosome', pa.string()),
        pa.field('location_start', pa.int64()),
        pa.field('some_other_data', pa.list_(pa.string()))
    ])

    sql = loader._generate_create_table_sql("staging.flat_table", flattened_schema, schema_overrides={})

    expected_sql = """CREATE TABLE staging.flat_table (
  "id" BIGINT,
  "location_chromosome" TEXT,
  "location_start" BIGINT,
  "some_other_data" JSONB
);"""
    assert sql.strip() == expected_sql.strip()


def test_postgres_loader_adheres_to_abc():
    """
    Tests that PostgresLoader correctly implements the DatabaseLoader ABC.
    """
    from py_load_opentargets.loader import DatabaseLoader

    # 1. Check if it's a subclass. This verifies the inheritance.
    assert issubclass(PostgresLoader, DatabaseLoader)

    # 2. Check if it can be instantiated. This fails if any abstract methods
    #    are not implemented, which is the core check for the contract.
    try:
        PostgresLoader()
    except TypeError as e:
        pytest.fail(f"PostgresLoader failed to instantiate. It might be missing implementation for an abstract method. Error: {e}")


def test_align_final_table_schema_detects_all_drift_types(loader, mocker, caplog):
    """
    Tests that align_final_table_schema correctly detects new columns,
    mismatched types, and removed columns, then takes the correct actions
    (adds new columns and warns about the others).
    """
    # 1. Setup
    # Mock the database connection and cursor
    mock_conn = mocker.patch('psycopg.connect').return_value
    mock_cursor = mock_conn.cursor.return_value

    # The psycopg Composed object's `as_string` method needs a context object
    # that has a `.connection.info.encoding` attribute. We must mock this structure.
    mock_info = mocker.MagicMock()
    mock_info.encoding = 'utf-8'
    mock_conn.connection.info = mock_info

    loader.conn = mock_conn
    loader.cursor = mock_cursor

    # Define the schemas that will be returned by the mocked DB query
    final_schema = {
        "id": "bigint",
        "score": "double precision", # This will be mismatched
        "description": "text",
        "old_column": "boolean" # This will be removed
    }
    staging_schema = {
        "id": "bigint",
        "score": "text", # Mismatched type
        "description": "text",
        "new_column": "jsonb" # New column
    }

    # Mock the method that queries the DB for schemas
    mocker.patch.object(
        loader,
        '_get_table_schema_from_db',
        side_effect=[staging_schema, final_schema]
    )

    # 2. Act
    loader.align_final_table_schema("staging.table", "final.table")

    # 3. Assert
    # a. Assert that ALTER TABLE was called to add the new column
    mock_cursor.execute.assert_called_once()
    # Instead of rendering the SQL to a string (which is complex to mock),
    # we inspect the representation of the Composed object to ensure the
    # correct components were used to build it.
    composed_sql = mock_cursor.execute.call_args.args[0]
    executed_sql_repr = repr(composed_sql)

    assert "SQL('ALTER TABLE ')" in executed_sql_repr
    assert "Identifier('final', 'table')" in executed_sql_repr
    assert "SQL(' ADD COLUMN ')" in executed_sql_repr
    assert "Identifier('new_column')" in executed_sql_repr
    assert "SQL('jsonb')" in executed_sql_repr

    # b. Assert that warnings were logged for mismatched types and removed columns
    assert "SCHEMA DRIFT DETECTED for column 'score'" in caplog.text
    assert "Staging table type is 'text', but final table type is 'double precision'" in caplog.text
    assert "exist in the final table 'final.table' but not in the new data source: old_column" in caplog.text
