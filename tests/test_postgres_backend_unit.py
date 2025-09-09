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

def test_get_transformed_schema_no_flattening(loader, mocker):
    """
    Tests that the schema transformation returns the original schema when no
    flattening is configured.
    """
    mocker.patch('psycopg.connect')
    original_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('nested_data', pa.struct([pa.field('a', pa.string())]))
    ])

    # Connect with no special config
    loader.connect("dummy_conn_str", dataset_config={})

    transformed_schema = loader._get_transformed_schema(original_schema)

    assert transformed_schema == original_schema

def test_get_transformed_schema_with_flattening(loader, mocker):
    """
    Tests that the schema transformation correctly flattens a specified struct.
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
        "flatten_structs": ["data_to_flatten"],
        "flatten_separator": "__"
    }
    loader.connect("dummy_conn_str", dataset_config=config)

    transformed_schema = loader._get_transformed_schema(original_schema)

    expected_schema = pa.schema([
        pa.field('id', pa.int64()),
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

    # No config needed for this method as it operates on the schema passed to it
    sql = loader._generate_create_table_sql("public.my_table", schema)

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
    # This schema would be the *output* of _get_transformed_schema
    flattened_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('location_chromosome', pa.string()),
        pa.field('location_start', pa.int64()),
        pa.field('some_other_data', pa.list_(pa.string())) # This should become JSONB
    ])

    sql = loader._generate_create_table_sql("staging.flat_table", flattened_schema)

    expected_sql = """CREATE TABLE staging.flat_table (
  "id" BIGINT,
  "location_chromosome" TEXT,
  "location_start" BIGINT,
  "some_other_data" JSONB
);"""
    assert sql.strip() == expected_sql.strip()
