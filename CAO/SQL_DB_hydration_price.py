# SQL_DB_hydration_price.py
import mysql.connector
from mysql.connector import errorcode
from logging_config import logger
import pandas as pd

class SQL_DB_Hydration_Price:
    def __init__(self, userName, passWord, host, dataBase, db_port, initializeTable=False, table_names=None):
        self.userName = userName
        self.passWord = passWord
        self.dataBase = dataBase
        self.port = db_port
        self.host = host
        
        # Default table names
        self.tables = {
            "Hydration_price": "Hydration_price",
            "Hydration_price_batches": "Hydration_price_batches"
        }
        
        # Override if custom names provided
        if table_names:
            self.tables.update(table_names)

        if initializeTable:
            self.initialize_tables()

    def initialize_tables(self):
        sql_command = f"""
        CREATE TABLE IF NOT EXISTS {self.tables['Hydration_price']} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            asset_id VARCHAR(50),
            symbol VARCHAR(50),
            price_usdt DOUBLE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.executeSQL(sql_command)

        sql_command_batch = f"""
        CREATE TABLE IF NOT EXISTS {self.tables['Hydration_price_batches']} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id BIGINT NOT NULL,
            data_hash VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.executeSQL(sql_command_batch)
        
        # Ensure hash column exists (idempotent check)
        table_name = self.tables['Hydration_price_batches']
        check_col_sql = """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = 'data_hash'
        """
        res = self.executeSQL(check_col_sql, (self.dataBase, table_name))
        if res and res[0][0] == 0:
            self.executeSQL(f"ALTER TABLE {table_name} ADD COLUMN data_hash VARCHAR(64);")
            logger.info(f"Added 'data_hash' column to {table_name}")

    def errorMessage(self, message):
        logger.error(f"SQL Error: {message}")

    def executeSQL(self, query, params=None):
        try:
            cnx = mysql.connector.connect(
                user=self.userName,
                password=self.passWord,
                host=self.host,
                database=self.dataBase,
                port=self.port
            )
            cursor = cnx.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            try:
                values = cursor.fetchall()
            except:
                values = None
            cnx.commit()
            cursor.close()
            cnx.close()
            return values
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(str(err))
            raise
        except Exception as err:
            logger.exception(f"Unexpected error in executeSQL: {err}")
            raise

    def get_last_price_hash(self):
        """Fetches the data_hash of the most recent price batch."""
        table_name = self.tables['Hydration_price_batches']
        query = f"SELECT data_hash FROM {table_name} ORDER BY id DESC LIMIT 1"
        result = self.executeSQL(query)
        if result and result[0][0]:
            return result[0][0]
        return None

    def update_hydration_prices(self, processed_data, batch_id, data_hash=None):
        if not processed_data:
            logger.warning("No data to store in Hydration_price table.")
            return
        
        df = pd.DataFrame(processed_data)
        table_name = self.tables['Hydration_price']
        table_batches = self.tables['Hydration_price_batches']
        
        for _, row in df.iterrows():
            values_list = [
                "NULL" if pd.isna(value) else
                "'" + str(value).replace("'", "\\'") + "'" if isinstance(value, str) else
                str(value)
                for value in [batch_id] + row[['asset_id', 'symbol', 'price_usdt']].tolist()
            ]
            values = ', '.join(values_list)
            
            query = f"""
            INSERT INTO {table_name} (
                batch_id, asset_id, symbol, price_usdt
            ) VALUES ({values})
            """
            self.executeSQL(query)
        
        # Track batch
        if data_hash:
            self.executeSQL(
                f"INSERT INTO {table_batches} (batch_id, data_hash) VALUES (%s, %s)",
                (batch_id, data_hash)
            )

        logger.info(f"Hydration prices stored in MySQL with batch_id {batch_id}")
