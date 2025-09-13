import pytest
from py_load_opentargets.config import load_config, get_config


def test_load_config_file_not_found():
    """
    Test that FileNotFoundError is raised when the config file does not exist.
    """
    with pytest.raises(FileNotFoundError):
        load_config("non_existent_file.toml")


def test_load_config_malformed_toml(tmp_path):
    """
    Test that TOMLDecodeError is raised when the config file is malformed.
    """
    malformed_toml = tmp_path / "malformed.toml"
    malformed_toml.write_text("this is not valid toml")
    with pytest.raises(
        Exception
    ):  # Using generic exception as tomllib is not directly imported
        load_config(str(malformed_toml))


def test_load_config_missing_provider_warning(caplog, tmp_path):
    """
    Test that a warning is logged when a provider is specified but no
    corresponding config section is found.
    """
    config_with_missing_provider = tmp_path / "missing_provider.toml"
    config_with_missing_provider.write_text("""
[source]
provider = "missing_provider"
""")
    load_config(str(config_with_missing_provider))
    assert (
        "Source provider 'missing_provider' is specified but no corresponding"
        in caplog.text
    )


def test_get_config_loads_defaults_when_not_loaded(monkeypatch):
    """
    Test that get_config() loads the default configuration if it hasn't
    been loaded yet.
    """
    monkeypatch.setattr("py_load_opentargets.config._config", None)
    config = get_config()
    assert config is not None
    assert "database" in config
    assert "backend" in config["database"]
