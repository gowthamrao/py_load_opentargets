import os
import logging
from typing import Dict, Any, Optional

try:
    import tomllib
except ImportError:
    import tomli as tomllib
from importlib import resources

logger = logging.getLogger(__name__)

# The global config object, initialized with defaults.
_config: Optional[Dict[str, Any]] = None

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Loads configuration from the default and user-provided TOML files.

    The configuration is loaded in the following order:
    1. The default configuration (`default_config.toml`) packaged with the library.
    2. A user-provided configuration file, which overrides the defaults.

    :param config_path: Path to a user-provided TOML configuration file.
    :return: A dictionary containing the merged configuration.
    """
    global _config

    # Load default config from package resources
    with resources.files('py_load_opentargets').joinpath('default_config.toml').open('rb') as f:
        default_config = tomllib.load(f)

    # If a user config is provided, load it and merge
    if config_path:
        logger.info(f"Loading user-provided configuration from: {config_path}")
        try:
            with open(config_path, 'rb') as f:
                user_config = tomllib.load(f)
            # A simple merge: user config keys overwrite default keys.
            # A more sophisticated deep merge could be used if needed.
            merged_config = default_config.copy()
            merged_config.update(user_config)
            _config = merged_config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except tomllib.TOMLDecodeError as e:
            logger.error(f"Error parsing TOML file {config_path}: {e}")
            raise
    else:
        logger.info("Using default configuration.")
        _config = default_config

    return _config

def get_config() -> Dict[str, Any]:
    """
    Returns the loaded configuration.

    If the configuration has not been loaded yet, it will be loaded with defaults.

    :return: The configuration dictionary.
    """
    if _config is None:
        load_config()
    return _config
