import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import polars as pl

from py_load_opentargets.backends.postgres import PostgresLoader, _ParquetStreamer

@pytest.fixture(scope="module")
def complex_parquet_file(tmp_path_factory) -> Path:
    """
    Creates a parquet file with a complex schema for testing transformations.
    This fixture has 'module' scope to avoid recreating the file for every test function.
    """
    tmp_dir = tmp_path_factory.mktemp("data")
    file_path = tmp_dir / "test_complex.parquet"

    # Define a schema with nested structs and lists
    schema = pa.schema([
        pa.field("id", pa.string(), metadata={"description": "The main ID"}),
        pa.field("info", pa.struct([
            pa.field("name", pa.string()),
            pa.field("license", pa.string())
        ])),
        pa.field("annotations", pa.struct([
            pa.field("source", pa.string()),
            pa.field("url", pa.string())
        ])),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("numeric_score", pa.float64())
    ])

    # Create some data
    data = pa.Table.from_pydict({
        "id": ["a-1"],
        "info": [{"name": "Test Data", "license": "CC0"}],
        "annotations": [{"source": "internal", "url": "http://example.com"}],
        "tags": [["tag1", "tag2"]],
        "numeric_score": [99.9]
    }, schema=schema)

    pq.write_table(data, file_path)
    return file_path

def test_get_transformed_schema_rename_and_flatten(complex_parquet_file):
    """
    Tests that the schema transformation correctly renames a column and flattens a struct.
    """
    loader = PostgresLoader()
    loader.dataset_config = {
        "schema_overrides": {
            "id": {"rename": "test_id"},
            "info": {"action": "flatten"}
        },
        "flatten_separator": "__" # Use a distinct separator for testing
    }

    original_schema = pq.read_schema(complex_parquet_file)
    transformed_schema = loader._get_transformed_schema(original_schema)

    # Check that 'id' was renamed to 'test_id'
    assert "test_id" in transformed_schema.names
    assert "id" not in transformed_schema.names
    assert transformed_schema.field("test_id").type == pa.string()

    # Check that 'info' was flattened
    assert "info__name" in transformed_schema.names
    assert "info__license" in transformed_schema.names
    assert "info" not in transformed_schema.names

    # Check that other fields are preserved
    assert "annotations" in transformed_schema.names
    assert "tags" in transformed_schema.names

def test_get_transformed_schema_json_serialization():
    """
    Tests that the schema transformation preserves nested types when the
    action is 'json', allowing them to be mapped to JSONB.
    """
    loader = PostgresLoader()
    loader.dataset_config = {
        "schema_overrides": {
            "annotations": {"action": "json"},
            "tags": {"action": "json"}
        }
    }

    original_schema = pa.schema([
        pa.field("annotations", pa.struct([pa.field("source", pa.string())])),
        pa.field("tags", pa.list_(pa.string()))
    ])

    transformed_schema = loader._get_transformed_schema(original_schema)

    # The types should be preserved, not converted to string.
    assert pa.types.is_struct(transformed_schema.field("annotations").type)
    assert pa.types.is_list(transformed_schema.field("tags").type)

def test_parquet_streamer_with_transformations(complex_parquet_file):
    """
    Tests the end-to-end data transformation through the _ParquetStreamer.
    """
    schema_overrides = {
        "id": {"rename": "test_id"},
        "info": {"action": "flatten"},
        "annotations": {"action": "json"},
        "tags": {"action": "json"}
    }
    flatten_separator = "_"

    original_schema = pq.read_schema(complex_parquet_file)

    streamer = _ParquetStreamer(
        paths=[str(complex_parquet_file)],
        original_schema=original_schema,
        schema_overrides=schema_overrides,
        flatten_separator=flatten_separator,
    )

    # Read the entire stream content
    stream_content = streamer.read()

    # The output is TSV, so we can parse it
    # Expected header based on transformations:
    # test_id, info_name, info_license, annotations, tags, numeric_score

    # Polars can read the TSV result for easy validation
    result_df = pl.read_csv(
        source=stream_content.encode(),
        separator='\t',
        has_header=False,
        new_columns=[
            "test_id", "info_name", "info_license", "annotations", "tags", "numeric_score"
        ]
    )

    assert result_df.shape == (1, 6)
    row = result_df.row(0, named=True)

    assert row["test_id"] == "a-1"
    assert row["info_name"] == "Test Data"
    assert row["info_license"] == "CC0"
    # Polars stringifies nested types when reading from Parquet,
    # so the JSON strings should match this behavior.
    assert row["annotations"] == '{"source":"internal","url":"http://example.com"}'
    assert row["tags"] == '["tag1","tag2"]'
    assert row["numeric_score"] == 99.9
