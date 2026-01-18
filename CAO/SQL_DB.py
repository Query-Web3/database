#######################################################################################################
# Name: SQL_DB class
# Purpose: Perform all kinds of searching operation through the database using Mysql
# Input:
# Output:
# Developed by: Dr. Cao
# Developed on: 1/2025
# Modified by:
# Change log:
#######################################################################################################


import mysql.connector
from mysql.connector import errorcode
import sys
import json
import datetime
from datetime import timedelta
import pandas as pd

import math
# we have one bot database for public, create the sql user name and password and 
# also the database, and save it in the .env file 

# need to: pip install mysql-connector-python


class SQL_DB:
    def __init__(self, db_config = None, userName = None, passWord = None, port = 3306, host="127.0.0.1", dataBase = None, initializeTable = False):
        '''
        Upon initialization, the DB_init class will connect to database
        You need to either set up db_config or userName and passWord
        '''
        if db_config is not None:
            ## Load configuration
            con_file = open(db_config)
            config = json.load(con_file)
            con_file.close()
            try:
                self.userName = config['user']
                self.passWord = config['pass']
                if "database" not in config:
                    self.dataBase = "BOTDatabase"
                else:
                    self.dataBase = config['database']

            except:
                self.errorMessage("Please set up user and pass in the configuration file "+db_config+", otherwise, we cannot connect to the DB")
                return
        else:
            if userName is None or passWord is None:
                self.errorMessage("If you don't provide the configuration file db_config, you must set up userName and passWord!")
                return
            else:
                self.userName = userName
                self.passWord = passWord
                self.dataBase = dataBase
                self.host = host
                self.port = port

        if initializeTable == True:
            #print("Warning, will drop the tables now")  we don't want to drop the table
            #self._dropAllTables()     # we will drop the table first and then do the rest, be careful here
            # Create a invite and inviter table
            # sql_command = "CREATE TABLE IF NOT EXISTS inviteTable(invitation_id VARCHAR(50), inviter_user_id VARCHAR(50) NOT NULL, invited_user_id VARCHAR(50) NOT NULL, guild_id VARCHAR(50) NOT NULL, date_added DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, primary key(inviter_user_id, invited_user_id,guild_id));"
            # self.executeSQL(sql_command)

            # create Bifrost site table
            sql_command = """CREATE TABLE IF NOT EXISTS Bifrost_site_table (
            auto_id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            Asset VARCHAR(255),
            Value DECIMAL(20,3),
            tvl DECIMAL(20,6),
            tvm DECIMAL(20,6),
            holders INT,
            apy DECIMAL(20,6),
            apyBase DECIMAL(20,6),
            apyReward DECIMAL(20,6),
            totalIssuance DECIMAL(20,6),
            holdersList TEXT,
            annualized_income DECIMAL(20,6),
            bifrost_staking_7day_apy DECIMAL(20,6),
            created DATETIME,
            daily_reward DECIMAL(20,6),
            exited_node INT,
            exited_not_transferred_node INT,
            exiting_online_node INT,
            gas_fee_income DECIMAL(20,6),
            id INT,
            mev_7day_apy VARCHAR(255),
            mev_apy DECIMAL(20,6),
            mev_income DECIMAL(20,6),
            online_node INT,
            slash_balance DECIMAL(20,6),
            slash_num INT,
            staking_apy DECIMAL(20,6),
            staking_income DECIMAL(20,6),
            total_apy DECIMAL(20,6),
            total_balance DECIMAL(20,6),
            total_effective_balance DECIMAL(20,6),
            total_node INT,
            total_reward DECIMAL(20,6),
            total_withdrawals DECIMAL(20,6),
            stakingApy DECIMAL(20,6),
            stakingIncome DECIMAL(20,6),
            mevApy DECIMAL(20,6),
            mevIncome DECIMAL(20,6),
            gasFeeApy VARCHAR(255),
            gasFeeIncome DECIMAL(20,6),
            totalApy DECIMAL(20,6),
            totalIncome DECIMAL(20,6),
            baseApy DECIMAL(20,6),
            farmingAPY DECIMAL(20,6),
            veth2TVS DECIMAL(20,6),
            apyMev DECIMAL(20,6),
            apyGas DECIMAL(20,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""

            # create Bifrost site table
            self.executeSQL(sql_command)

            # create Bifrost staking table 
            sql_command = """CREATE TABLE IF NOT EXISTS Bifrost_staking_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            contractAddress VARCHAR(255),
            symbol VARCHAR(50),
            slug VARCHAR(100),
            baseSlug VARCHAR(100),
            unstakingTime INT,
            users INT,
            apr DECIMAL(20,6),
            fee DECIMAL(20,6),
            price DECIMAL(20,6),
            exchangeRatio DECIMAL(20,6),
            supply DECIMAL(20,6),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
            # create Bifrost staking table
            self.executeSQL(sql_command)


            # create Bifrost batch ID table 
            sql_command = """CREATE TABLE IF NOT EXISTS Bifrost_batchID_table (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id INT NOT NULL,
            chain VARCHAR(25),
            status VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
            # create Bifrost staking table
            self.executeSQL(sql_command)

    def errorMessage(self,message):
        print("error message:" , message)


    def executeSQL(self, query,params=None):
        try:
            self.cnx = mysql.connector.connect(
                user=self.userName, 
                password=self.passWord, 
                host=self.host,
                database=self.dataBase,
                port=self.port
            )
            cursor = self.cnx.cursor()
            if params == None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            #print("Now execute the query " + query)
            try:
                values = cursor.fetchall()
            except:
                values = None
            self.cnx.commit()      # this is important, otherwise you cannot invert the record
            #print("done! nothing happened?")
            cursor.close()
            self.cnx.close()
            return values
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self.errorMessage("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self.errorMessage("Database does not exist")
            else:
                self.errorMessage(err)
        except mysql.connector.Error as err:
            logging.error(err)


    def update_bifrost_database(self, df1, df2, batch_id):
        """
        Updates the database with the records from two dataframes using the same batch_id.
        
        Parameters:
        - df1: The first dataframe containing columns related to general assets.
        - df2: The second dataframe containing specific asset data.
        - df3: The bifrost batch ID table
        - batch_id: A unique ID for this batch of data insertion.
        """
        # Define the table names
        table1 = "Bifrost_site_table"
        table2 = "Bifrost_staking_table"
        table3 = "Bifrost_batchID_table"

        # ---------- helper to clean + convert a single value ----------
        def clean_value(val):
            # Lists -> JSON string
            if isinstance(val, list):
                return json.dumps(val)

            # None / NaN -> None (becomes NULL in MySQL)
            if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
                return None

            # Pandas-style NaN / NaT detection for any type
            try:
                if pd.isna(val):
                    return None
            except TypeError:
                # pd.isna() can raise on some non-numeric types, ignore
                pass

            # Otherwise keep value as-is (str, int, float, etc.)
            return val

        # ============= INSERT df1 into Bifrost_site_table =============
        if df1 is not None and len(df1) > 0:
            cols1 = df1.columns.tolist()
            # +1 for batch_id
            placeholders1 = ", ".join(["%s"] * (len(cols1) + 1))
            col_names1 = ", ".join(["batch_id"] + cols1)
            query1 = f"INSERT INTO {table1} ({col_names1}) VALUES ({placeholders1})"

            for _, row in df1.iterrows():
                params = [batch_id]
                for val in row.tolist():
                    params.append(clean_value(val))
                # Use parameterized query
                self.executeSQL(query1, params)

        # ============= INSERT df2 into Bifrost_staking_table ==========
        if df2 is not None and len(df2) > 0:
            cols2 = df2.columns.tolist()
            placeholders2 = ", ".join(["%s"] * (len(cols2) + 1))
            col_names2 = ", ".join(["batch_id"] + cols2)
            query2 = f"INSERT INTO {table2} ({col_names2}) VALUES ({placeholders2})"

            for _, row in df2.iterrows():
                params = [batch_id]
                for val in row.tolist():
                    params.append(clean_value(val))
                self.executeSQL(query2, params)

        # ============= INSERT batch_id into Bifrost_batchID_table =====
        query3 = f"INSERT INTO {table3} (batch_id, chain, status) VALUES (%s, %s, %s)"
        self.executeSQL(query3, (batch_id, "Bifrost", "F"))

        print(f"Records successfully updated for batch_id {batch_id}.")
