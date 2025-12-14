# SQL_DB_hydration.py
import mysql.connector
from mysql.connector import errorcode
import logging
import pandas as pd

class SQL_DB_Hydration:
    def __init__(self, userName, passWord, host, dataBase, db_port=3306, initializeTable=False):
        self.userName = userName
        self.passWord = passWord
        self.dataBase = dataBase
        self.port = db_port
        self.host = host

        if initializeTable:
            self.initialize_tables()

    def initialize_tables(self):
        sql_command = """
        CREATE TABLE IF NOT EXISTS hydration_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            asset_id VARCHAR(50),
            symbol VARCHAR(50),
            farm_apr DOUBLE,
            pool_apr DOUBLE,
            total_apr DOUBLE,
            tvl_usd DOUBLE,
            volume_usd DOUBLE,
            timestamp VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.executeSQL(sql_command)

    def errorMessage(self, message):
        print("error message: " + message)

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
            logging.error(err)
            raise

    def update_hydration_database(self, processed_data, batch_id):
        if not processed_data:
            print("No data to store in the database.")
            return
        
        df = pd.DataFrame(processed_data)
        table_name = "hydration_data"
        
        for _, row in df.iterrows():
            values_list = [
                "NULL" if pd.isna(value) else
                "'" + str(value).replace("'", "\\'") + "'" if isinstance(value, str) else
                str(value)
                for value in [batch_id] + row.tolist()
            ]
            values = ', '.join(values_list)
            
            query = f"""
            INSERT INTO {table_name} (
                batch_id, asset_id, symbol, farm_apr, pool_apr, total_apr,
                tvl_usd, volume_usd, timestamp
            ) VALUES ({values})
            """
            self.executeSQL(query)
        
        print(f"Hydration data stored in MySQL database with batch_id {batch_id}")
