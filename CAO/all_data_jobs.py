#!/usr/bin/env python3
import os
import subprocess
import sys
import time
from pathlib import Path
from dotenv import load_dotenv
from logging_config import logger

from db_migration.migration import Migration
from SQL_DB_stella import SQL_DB_Stella
from SQL_DB import SQL_DB
from SQL_DB_hydration import SQL_DB_Hydration
from SQL_DB_hydration_price import SQL_DB_Hydration_Price

# Base directory where all the scripts live
BASE_DIR = Path(__file__).resolve().parent

# Script paths
BIFROST_SCRIPT = BASE_DIR / "Bifrost_Data_fetching.py"
HYDRATION_SCRIPT = BASE_DIR / "Hydration_Data_fetching.py"
ASSET_PRICES_SCRIPT = BASE_DIR / "fetch_asset_prices.py"
MERGE_SCRIPT = BASE_DIR / "combine_tables.py"


def start_long_running_scripts():
    """
    Start the three fetch scripts that run their own internal loops.
    Returns a list of Popen processes.
    """
    processes = []

    commands = [
        [sys.executable, str(BIFROST_SCRIPT)],
        [sys.executable, str(HYDRATION_SCRIPT)],
        [sys.executable, str(ASSET_PRICES_SCRIPT)],
    ]

    for cmd in commands:
        logger.info(f"Starting: {' '.join(cmd)}")
        p = subprocess.Popen(cmd, cwd=str(BASE_DIR))
        processes.append(p)

    return processes


def run_merge_script():
    """
    Run merge_multiple_tables.py once (blocking).
    """
    logger.info("Running merge_multiple_tables.py ...")
    try:
        subprocess.run(
            [sys.executable, str(MERGE_SCRIPT)],
            cwd=str(BASE_DIR),
            check=True
        )
        logger.info("Merge completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Merge failed with exit code {e.returncode}")


def main():
    initialize_tables()
    # Start the three long-running fetch scripts
    processes = start_long_running_scripts()

    # Run merge_multiple_tables.py every hour
    MERGE_INTERVAL_SEC = 60 * 60  # 1 hour
    next_merge_time = time.time()  # run once immediately on start

    logger.info("Scheduler started. Press Ctrl+C to stop.")

    try:
        while True:
            now = time.time()

            # Optional: check if any fetch process died
            for p in processes:
                if p.poll() is not None:
                    logger.warning(f"Process {p.args} exited with code {p.returncode}")
                    # If you want auto-restart, you could restart it here.

            time.sleep(10)  # avoid busy loop

            # Run merge script when it's time
            if now >= next_merge_time:
                run_merge_script()
                next_merge_time = now + MERGE_INTERVAL_SEC

    except KeyboardInterrupt:
        logger.info("Received Ctrl+C, stopping all processes...")

    finally:
        # Try to gracefully stop child processes
        for p in processes:
            if p.poll() is None:
                try:
                    p.terminate()
                except Exception:
                    pass

        time.sleep(2)
        for p in processes:
            if p.poll() is None:
                try:
                    p.kill()
                except Exception:
                    pass

        logger.info("All processes stopped.")


def initialize_tables():
    load_dotenv()
    db_user = os.getenv("DB_USERNAME", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_name = os.getenv("DB_NAME", "quantDATA")
    db_port = os.getenv("DB_PORT",3306)

    SQL_DB_Hydration_Price(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        initializeTable=True,
        db_port=db_port,
        host=db_host
    )

    SQL_DB_Hydration(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        initializeTable=True,
        db_port=db_port,
        host=db_host
    )

    SQL_DB(
        userName = db_user, 
        passWord = db_password, 
        dataBase = db_name, 
        host=db_host, 
        port = db_port, 
        initializeTable=True
    )

    SQL_DB_Stella(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        db_port=db_port,
        host=db_host,
        initializeTable=True
    )

    with Migration(user=db_user, password=db_password, host=db_host, database=db_name, port=db_port, code_version=1) as migrator:
        migrator.migrate()


if __name__ == "__main__":
    main()

