
#!/usr/bin/env python3
# merge_multiple_tables.py
# Thin CLI wrapper using dotenv-based envs

from dotenv import load_dotenv
import os
from SQL_DB_mergeTables import SQL_DB_MergeTables

from utils import retry
from logging_config import logger

@retry(max_retries=3, delay=5)
def run_merge_process(db_user, db_password, db_name, db_port, db_host, init_tables):
    db = SQL_DB_MergeTables(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        port=db_port,
        host=db_host,
        initializeTable=init_tables
    )
    db.run_merge()

def main():
    # Load environment variables from .env file (if present)
    load_dotenv()

    try:
        db_user = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")
        db_port = os.getenv("DB_PORT",3306)
        db_host = os.getenv("DB_HOST", "127.0.0.1")
        init_tables = "1"

        run_merge_process(db_user, db_password, db_name, db_port, db_host, init_tables)

    except Exception as e:
        logger.exception(f"Fatal error in merge_multiple_tables: {e}")
        # Optionally exit with error code if managed by a supervisor
        # sys.exit(1)

if __name__ == "__main__":
    main()
