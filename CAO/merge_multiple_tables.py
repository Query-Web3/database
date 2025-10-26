
#!/usr/bin/env python3
# merge_multiple_tables.py
# Thin CLI wrapper using dotenv-based envs

from dotenv import load_dotenv
import os
from SQL_DB_mergeTables import SQL_DB_MergeTables

def main():
    # Load environment variables from .env file (if present)
    load_dotenv()

    db_user = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_port = os.getenv("DB_PORT",3306)
    init_tables = "1"

    db = SQL_DB_MergeTables(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        port=db_port,
        initializeTable=init_tables
    )
    db.run_merge()

if __name__ == "__main__":
    main()
