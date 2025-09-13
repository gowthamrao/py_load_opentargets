from py_load_opentargets.config import deep_merge, load_config

def test_deep_merge_simple():
    d = {'a': 1, 'b': {'c': 2}}
    u = {'b': {'d': 3}, 'e': 4}
    expected = {'a': 1, 'b': {'c': 2, 'd': 3}, 'e': 4}
    assert deep_merge(d, u) == expected

def test_deep_merge_overwrite():
    d = {'a': 1, 'b': {'c': 2}}
    u = {'a': 5, 'b': {'c': 6}}
    expected = {'a': 5, 'b': {'c': 6}}
    assert deep_merge(d, u) == expected

def test_deep_merge_with_new_keys():
    d = {'a': 1}
    u = {'b': {'c': 2}}
    expected = {'a': 1, 'b': {'c': 2}}
    assert deep_merge(d, u) == expected

def test_load_config_with_s3_provider(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[source]
provider = "s3"

[source.s3]
data_uri_template = "s3://my-bucket/{dataset_name}"
data_download_uri_template = "s3://my-bucket/{dataset_name}/"
checksum_uri_template = "s3://my-bucket/checksums/{version}"
""")
    config = load_config(str(config_path))
    assert config['source']['data_uri_template'] == "s3://my-bucket/{dataset_name}"
    assert config['source']['data_download_uri_template'] == "s3://my-bucket/{dataset_name}/"
    assert config['source']['checksum_uri_template'] == "s3://my-bucket/checksums/{version}"

def test_load_config_with_gcs_ftp_provider(tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[source]
provider = "gcs_ftp"

[source.gcs_ftp]
data_uri_template = "gs://my-bucket/{dataset_name}"
data_download_uri_template = "gs://my-bucket/{dataset_name}/"
checksum_uri_template = "ftp://my-ftp/{version}"
""")
    config = load_config(str(config_path))
    assert config['source']['data_uri_template'] == "gs://my-bucket/{dataset_name}"
    assert config['source']['data_download_uri_template'] == "gs://my-bucket/{dataset_name}/"
    assert config['source']['checksum_uri_template'] == "ftp://my-ftp/{version}"
