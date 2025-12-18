\
#!/usr/bin/env python3
# combine_tables.py
"""
Thin CLI wrapper that calls SQL_DB_combinedTables to take the latest batches
from hydration_data, pool_data, and the Bifrost table and append them into full_table.
"""

import os
from dotenv import load_dotenv
from SQL_DB_stella import SQL_DB_Stella
from SQL_DB import SQL_DB
from SQL_DB_hydration import SQL_DB_Hydration
from SQL_DB_hydration_price import SQL_DB_Hydration_Price
from SQL_DB_combinedTables import SQL_DB_CombinedTables

def main():
    load_dotenv()
    user = os.getenv("DB_USERNAME", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    db = os.getenv("DB_NAME", "quantDATA")
    db_port = os.getenv("DB_PORT",3306)

    initialize_tables(db_user=user, db_password=password, db_port=db_port, db_name=db, db_host=host)

    combiner = SQL_DB_CombinedTables(user=user, password=password, db_port=db_port, db=db, host=host)
    combiner.run_once()

def initialize_tables(db_user, db_password, db_port, db_name, db_host):
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


if __name__ == "__main__":
    main()
