\
#!/usr/bin/env python3
# combine_tables.py
"""
Thin CLI wrapper that calls SQL_DB_combinedTables to take the latest batches
from hydration_data, pool_data, and the Bifrost table and append them into full_table.
"""

import os
from dotenv import load_dotenv
from SQL_DB_combinedTables import SQL_DB_CombinedTables

def main():
    load_dotenv()
    user = os.getenv("DB_USERNAME", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    db = os.getenv("DB_NAME", "quantDATA")
    db_port = os.getenv("DB_PORT",3306)

    combiner = SQL_DB_CombinedTables(user=user, password=password, db_port=db_port, db=db, host=host)
    combiner.run_once()

if __name__ == "__main__":
    main()
