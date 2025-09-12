import logging
import sys
from pythonjsonlogger.json import JsonFormatter

def setup_logging(level=logging.INFO, json_format=False):
    """
    Configures the root logger for the application.

    This function can set up either standard text logging or structured
    JSON logging based on the 'json_format' parameter. It removes any
    existing handlers to ensure a clean setup.
    """
    logger = logging.getLogger()
    logger.setLevel(level)

    # Remove any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a handler that writes to stdout
    handler = logging.StreamHandler(sys.stdout)

    if json_format:
        # Use a JSON formatter
        formatter = JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s'
        )
        handler.setFormatter(formatter)
        log_message = "Structured JSON logging initialized."
    else:
        # Use a standard text formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        log_message = "Standard text logging initialized."

    # Add the configured handler to the root logger
    logger.addHandler(handler)

    # Redirect warnings from the 'warnings' module to the logging system
    logging.captureWarnings(True)
    warnings_logger = logging.getLogger('py.warnings')
    warnings_logger.addHandler(handler)
    warnings_logger.setLevel(logging.WARNING)

    logging.getLogger(__name__).info(log_message)
