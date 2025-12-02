# SQL_DB_stella.py
import mysql.connector
from mysql.connector import errorcode
import logging
import pandas as pd

class SQL_DB_Stella:
    def __init__(self, userName, passWord, dataBase, db_port=3306, initializeTable=False):
        self.userName = userName
        self.passWord = passWord
        self.dataBase = dataBase
        self.port = db_port

        if initializeTable:
            self.initialize_tables()

    def initialize_tables(self):
        sql_command = """
        CREATE TABLE IF NOT EXISTS pool_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            pool_id VARCHAR(255),
            token0_id VARCHAR(255),
            token0_symbol VARCHAR(50),
            token0_name VARCHAR(255),
            token0_decimals INT,
            token1_id VARCHAR(255),
            token1_symbol VARCHAR(50),
            token1_name VARCHAR(255),
            token1_decimals INT,
            liquidity DOUBLE,
            sqrt_price DOUBLE,
            tick INT,
            volume_usd_current DOUBLE,
            volume_usd_24h_ago DOUBLE,
            volume_usd_24h DOUBLE,
            tx_count INT,
            fees_usd_current DOUBLE,
            fees_usd_24h_ago DOUBLE,
            fees_usd_24h DOUBLE,
            amount_token0 DOUBLE,
            amount_token1 DOUBLE,
            pools_apr DOUBLE,
            farming_apr DOUBLE,
            final_apr DOUBLE,
            token_rewards TEXT,
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
                host='127.0.0.1',
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

    def update_pool_database(self, processed_data, batch_id):
        if not processed_data:
            print("No data to store in the database.")
            return
        
        df = pd.DataFrame(processed_data)
        table_name = "pool_data"
        
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
                batch_id, pool_id, token0_id, token0_symbol, token0_name, token0_decimals,
                token1_id, token1_symbol, token1_name, token1_decimals, liquidity,
                sqrt_price, tick, volume_usd_current, volume_usd_24h_ago, volume_usd_24h,
                tx_count, fees_usd_current, fees_usd_24h_ago, fees_usd_24h, amount_token0,
                amount_token1, pools_apr, farming_apr, final_apr, token_rewards, timestamp
            ) VALUES ({values})
            """
            self.executeSQL(query)
        
        print(f"Pool data stored in MySQL database with batch_id {batch_id}")
