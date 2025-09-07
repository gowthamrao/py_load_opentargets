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
