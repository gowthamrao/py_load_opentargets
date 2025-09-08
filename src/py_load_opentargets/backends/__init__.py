import logging
from importlib.metadata import entry_points
from ..loader import DatabaseLoader

logger = logging.getLogger(__name__)

def get_loader(backend_name: str) -> DatabaseLoader:
    """
    Finds and instantiates a database loader class using entry points.

    This function looks for installed packages that provide an entry point
    in the 'py_load_opentargets.backends' group.

    :param backend_name: The name of the backend to load (e.g., 'postgres').
    :return: An instance of the requested DatabaseLoader.
    :raises ValueError: If the requested backend is not found.
    """
    logger.info(f"Attempting to load database backend: '{backend_name}'")

    try:
        # Python 3.10+ provides a 'group' argument directly.
        # For older versions, one would iterate over all entry points.
        discovered_backends = {ep.name: ep for ep in entry_points(group='py_load_opentargets.backends')}
    except Exception as e:
        logger.error(f"Failed to discover entry points: {e}", exc_info=True)
        raise ValueError("Could not discover backend entry points. Check your installation.")

    if backend_name not in discovered_backends:
        logger.error(f"Backend '{backend_name}' not found. Available backends: {list(discovered_backends.keys())}")
        raise ValueError(f"Unsupported database backend: {backend_name}")

    entry_point = discovered_backends[backend_name]
    logger.info(f"Found entry point '{entry_point.name}' -> '{entry_point.value}'")

    try:
        loader_class = entry_point.load()
        return loader_class()
    except Exception as e:
        logger.error(f"Failed to load class for backend '{backend_name}': {e}", exc_info=True)
        raise ImportError(f"Could not load the class for the '{backend_name}' backend.")
