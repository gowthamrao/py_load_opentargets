import logging
import sys
from pythonjsonlogger import jsonlogger

def setup_logging():
    """
    Sets up structured JSON logging for the application.

    This function configures the root logger to output logs in JSON format
    to standard output. It removes any existing handlers to ensure that
    only the JSON handler is used.
    """
    logger = logging.getLogger()

    # Remove any existing handlers to avoid duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a handler that writes to stdout
    log_handler = logging.StreamHandler(sys.stdout)

    # Create a JSON formatter and add it to the handler
    # The format string defines the fields that will be in the JSON output.
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    log_handler.setFormatter(formatter)

    # Add the handler to the root logger
    logger.addHandler(log_handler)

    # Set the default logging level
    logger.setLevel(logging.INFO)

    # Redirect warnings from the 'warnings' module to the logging system
    logging.captureWarnings(True)

    # Also configure the root logger for the warnings module
    warnings_logger = logging.getLogger('py.warnings')
    warnings_logger.addHandler(log_handler)
    warnings_logger.setLevel(logging.WARNING)

    logging.getLogger(__name__).info("Structured JSON logging initialized.")
