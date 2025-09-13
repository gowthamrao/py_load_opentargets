import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([
    pa.field('id', pa.string()),
    pa.field('toxicityClass', pa.string()),
    pa.field('chemblIds', pa.list_(pa.string())),
    pa.field('country', pa.string()),
    pa.field('description', pa.string()),
    pa.field('warningType', pa.string()),
    pa.field('year', pa.int64()),
    pa.field('meddraSocCode', pa.int64()),
    pa.field('references', pa.list_(pa.struct([
        pa.field('source', pa.string()),
        pa.field('ids', pa.list_(pa.string()))
    ])))
])

data = {
    'id': ['CHEMBL123'],
    'toxicityClass': ['SomeClass'],
    'chemblIds': [['CHEMBL123']],
    'country': ['USA'],
    'description': ['A test warning'],
    'warningType': ['TEST'],
    'year': [2022],
    'meddraSocCode': [123],
    'references': [[{'source': 'test', 'ids': ['1', '2']}]]
}

table = pa.Table.from_pydict(data, schema=schema)
pq.write_table(table, 'tests/resources/drug_warnings.parquet')
