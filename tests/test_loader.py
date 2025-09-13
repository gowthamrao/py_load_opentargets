import pytest
from py_load_opentargets.loader import DatabaseLoader
import pyarrow as pa

# A minimal concrete implementation of the abstract base class to test it.
class ConcreteLoader(DatabaseLoader):
    def connect(self, conn_str, dataset_config=None): pass
    def get_foreign_keys(self, table_name): pass
    def drop_foreign_keys(self, table_name, foreign_keys): pass
    def recreate_foreign_keys(self, table_name, foreign_keys): pass
    def cleanup(self): pass
    def get_last_successful_version(self, dataset): pass
    def update_metadata(self, version, dataset, success, row_count, error_message=None): pass
    def prepare_staging_schema(self, schema_name): pass
    def prepare_staging_table(self, table_name, schema): pass
    def bulk_load_native(self, table_name, parquet_uris, schema): pass
    def table_exists(self, table_name): pass
    def align_final_table_schema(self, staging_table, final_table): pass
    def get_table_indexes(self, table_name): pass
    def drop_indexes(self, indexes): pass
    def recreate_indexes(self, indexes): pass
    def execute_merge_strategy(self, staging_table, final_table, primary_keys): pass
    def full_refresh_from_staging(self, staging_table, final_table, primary_keys): pass

@pytest.fixture
def loader_instance():
    return ConcreteLoader()

def test_connect_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).connect("dummy_conn_str")

def test_get_foreign_keys_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).get_foreign_keys("dummy_table")

def test_drop_foreign_keys_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).drop_foreign_keys("dummy_table", [])

def test_recreate_foreign_keys_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).recreate_foreign_keys("dummy_table", [])

def test_cleanup_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).cleanup()

def test_get_last_successful_version_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).get_last_successful_version("dummy_dataset")

def test_update_metadata_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).update_metadata("v1", "ds1", True, 100)

def test_prepare_staging_schema_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).prepare_staging_schema("dummy_schema")

def test_prepare_staging_table_raises_not_implemented(loader_instance):
    schema = pa.schema([pa.field('foo', pa.int64())])
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).prepare_staging_table("dummy_table", schema)

def test_bulk_load_native_raises_not_implemented(loader_instance):
    schema = pa.schema([pa.field('foo', pa.int64())])
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).bulk_load_native("dummy_table", [], schema)

def test_table_exists_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).table_exists("dummy_table")

def test_align_final_table_schema_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).align_final_table_schema("staging", "final")

def test_get_table_indexes_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).get_table_indexes("dummy_table")

def test_drop_indexes_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).drop_indexes([])

def test_recreate_indexes_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).recreate_indexes([])

def test_execute_merge_strategy_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).execute_merge_strategy("staging", "final", ["id"])

def test_full_refresh_from_staging_raises_not_implemented(loader_instance):
    with pytest.raises(NotImplementedError):
        super(ConcreteLoader, loader_instance).full_refresh_from_staging("staging", "final", ["id"])
