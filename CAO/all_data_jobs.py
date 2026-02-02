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
from SQL_DB_hydration import SQL_DB_Hydration
from SQL_DB_hydration_price import SQL_DB_Hydration_Price
from utils import HealthMonitor

import signal

# Base directory where all the scripts live
BASE_DIR = Path(__file__).resolve().parent

# Default script paths
BIFROST_SCRIPT = BASE_DIR / "Bifrost_Data_fetching.py"
HYDRATION_SCRIPT = BASE_DIR / "Hydration_Data_fetching.py"
ASSET_PRICES_SCRIPT = BASE_DIR / "fetch_asset_prices.py"
MERGE_SCRIPT = BASE_DIR / "combine_tables.py"

class JobOrchestrator:
    def __init__(self, scripts=None, merge_script=None):
        self.scripts = scripts if scripts is not None else [BIFROST_SCRIPT, HYDRATION_SCRIPT, ASSET_PRICES_SCRIPT]
        self.merge_script = merge_script or MERGE_SCRIPT
        self.processes = []
        self.running = False
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, sig, frame):
        logger.info(f"Received signal {sig}, stopping orchestrator...")
        self.running = False

    def start_long_running_scripts(self):
        """
        Start the fetch scripts that run their own internal loops.
        """
        for script in self.scripts:
            cmd = [sys.executable, str(script)]
            logger.info(f"Starting: {' '.join(cmd)}")
            p = subprocess.Popen(cmd, cwd=str(BASE_DIR))
            self.processes.append(p)
        return self.processes

    def run_merge_script(self):
        """
        Run merge script once (blocking).
        """
        logger.info(f"Running merge script: {self.merge_script}")
        try:
            subprocess.run(
                [sys.executable, str(self.merge_script)],
                cwd=str(BASE_DIR),
                check=True
            )
            logger.info("Merge completed successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Merge failed with exit code {e.returncode}")
        except Exception as e:
            logger.error(f"Merge error: {e}")

    def stop_all(self):
        """
        Gracefully stop all child processes.
        """
        logger.info("Stopping all child processes...")
        # 1. Terminate
        for p in self.processes:
            if p.poll() is None:
                try:
                    p.terminate()
                except Exception as e:
                    logger.debug(f"Error terminating process {p.pid}: {e}")

        # 2. Wait and kill if necessary
        time.sleep(2)
        for p in self.processes:
            if p.poll() is None:
                try:
                    logger.warning(f"Process {p.pid} did not stop gracefully, killing it...")
                    p.kill()
                except Exception as e:
                    logger.error(f"Error killing process {p.pid}: {e}")
        
        self.processes = []
        logger.info("Orchestrator cleanup complete.")

    def run(self, merge_interval_sec=3600, max_iterations=None):
        """
        Main orchestration loop.
        Args:
            merge_interval_sec (int): Frequency of merge script execution.
            max_iterations (int/None): If set, limits the number of 10s sleep cycles.
        """
        self.running = True
        initialize_tables()
        self.start_long_running_scripts()

        next_merge_time = time.time()  # run once immediately on start
        iterations = 0

        logger.info(f"Scheduler started (Merge Interval: {merge_interval_sec}s).")

        try:
            while self.running:
                now = time.time()
                
                # 1. Health Checks
                db_config = {
                    'user': os.getenv("DB_USERNAME"),
                    'password': os.getenv("DB_PASSWORD"),
                    'database': os.getenv("DB_NAME"),
                    'port': int(os.getenv("DB_PORT", 3306)),
                    'host': os.getenv("DB_HOST", "127.0.0.1")
                }
                if not HealthMonitor.check_db_connection(db_config):
                    logger.error("Health Check Failed: Database unreachable!")

                # 2. Process Monitoring
                for p in self.processes:
                    if p.poll() is not None:
                        logger.warning(f"Process {p.args} exited with code {p.returncode}")
                        # In a production setting, we might want to restart here.
                        # For now, we just log it.

                # 3. Handle Merge
                if now >= next_merge_time:
                    self.run_merge_script()
                    next_merge_time = now + merge_interval_sec

                # 4. Sleep and check exit
                time.sleep(10)
                
                iterations += 1
                if max_iterations and iterations >= max_iterations:
                    logger.info("Max iterations reached, stopping...")
                    break
        finally:
            self.stop_all()


def main():
    orchestrator = JobOrchestrator()
    orchestrator.run()


def initialize_tables():
    load_dotenv()
    db_user = os.getenv("DB_USERNAME", "root")
    db_password = os.getenv("DB_PASSWORD", "")
    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_name = os.getenv("DB_NAME", "quantDATA")
    db_port = int(os.getenv("DB_PORT", 3306))

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
        userName=db_user, 
        passWord=db_password, 
        dataBase=db_name, 
        host=db_host, 
        port=db_port, 
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

