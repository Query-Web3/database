import time
import functools
import shutil
import mysql.connector
from logging_config import logger

def retry(max_retries=3, delay=2, backoff=2, exceptions=(Exception,)):
    """
    Decorator to retry a function call if it raises specified exceptions.
    
    Args:
        max_retries (int): Maximum number of retries before giving up.
        delay (int/float): Initial delay between retries in seconds.
        backoff (int/float): Multiplier applied to delay after each retry.
        exceptions (tuple): Tuple of exceptions to catch and retry on.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt < max_retries:
                        logger.warning(
                            f"Function '{func.__name__}' failed with error: {e}. "
                            f"Retrying in {current_delay} seconds... (Attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"Function '{func.__name__}' failed after {max_retries} retries. "
                            f"Last error: {e}"
                        )
                        raise
        return wrapper
    return decorator

class HealthMonitor:
    @staticmethod
    def check_db_connection(db_config):
        """
        Checks if a database connection can be established.
        Args:
            db_config (dict): Dictionary with keys: user, password, host, database, port.
        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            conn = mysql.connector.connect(**db_config)
            if conn.is_connected():
                conn.close()
                return True
            return False
        except Exception as e:
            logger.error(f"Health Check - DB Connection Failed: {e}")
            return False

    @staticmethod
    def check_process(process):
        """
        Checks if a subprocess is still running.
        Args:
            process (subprocess.Popen): The process object to check.
        Returns:
            bool: True if running, False if terminated.
        """
        if process.poll() is None:
            return True
        return False

    @staticmethod
    def check_disk_space(path="/", threshold_percent=90):
        """
        Checks if disk usage is below a threshold.
        Args:
            path (str): Path to check disk usage for.
            threshold_percent (int): Maximum allowed usage percentage.
        Returns:
            bool: True if space is healthy (usage < threshold), False otherwise.
        """
        try:
            total, used, free = shutil.disk_usage(path)
            usage_percent = (used / total) * 100
            if usage_percent > threshold_percent:
                logger.warning(f"Health Check - Disk Space Warning: {usage_percent:.2f}% used")
                return False
            return True
        except Exception as e:
            logger.error(f"Health Check - Disk Space Check Failed: {e}")
            return False
