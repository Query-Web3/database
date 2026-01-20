import logging
import os
import sys

def setup_logging(name=None, level=logging.INFO):
    """
    Sets up a standard logging configuration.
    Returns a logger instance.
    """
    # Create logger
    logger = logging.getLogger(name or "QueryWeb3")
    
    # If logger already has handlers, don't add more (prevents duplicate logs)
    if not logger.handlers:
        logger.setLevel(os.getenv("LOG_LEVEL", level))

        # Create console handler and set level
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add formatter to ch
        ch.setFormatter(formatter)

        # Add ch to logger
        logger.addHandler(ch)

        # Optionally add file handler
        log_file = os.getenv("LOG_FILE")
        if log_file:
            fh = logging.FileHandler(log_file)
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

    return logger

# Default logger for simple imports
logger = setup_logging()
