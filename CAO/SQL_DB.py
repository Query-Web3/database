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
# we have one bot database for public 
# CREATE USER 'queryweb3'@'localhost' IDENTIFIED BY '!SDCsdin2df2@';
# CREATE DATABASE QUERYWEB3;
# GRANT ALL PRIVILEGES ON QUERYWEB3.* TO 'queryweb3'@'localhost';
# FLUSH PRIVILEGES;
# user name: queryweb3
# pass:      !SDCsdin2df2@
# database:  QUERYWEB3

# need to: pip install mysql-connector-python


class SQL_DB:
    def __init__(self, db_config = None, userName = None, passWord = None, port = 3306, dataBase = None, initializeTable = False):
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
        else:
            if userName is None or passWord is None:
                self.errorMessage("If you don't provide the configuration file db_config, you must set up userName and passWord!")
            else:
                self.userName = userName
                self.passWord = passWord
                self.dataBase = dataBase
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
            mev_7day_apy DECIMAL(20,6),
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
            gasFeeApy DECIMAL(20,6),
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
        print("error message:" + message)


    def executeSQL(self, query,params=None):
        try:
            self.cnx = mysql.connector.connect(
                user=self.userName, 
                password=self.passWord, 
                host='127.0.0.1',
                database=self.dataBase,
                port=self.port,
                ssl_disabled=True
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
        
        # Insert records from df1 into Bifrost_site_table table
        for _, row in df1.iterrows():
            # Safely format the values as a comma-separated string
            values_list = []
            for value in [batch_id] + row.tolist():
                if isinstance(value, list):  # Convert lists to JSON strings
                    values_list.append("'" + json.dumps(value).replace("'", "\\'") + "'")
                elif pd.isna(value) or value is None:  # Handle missing values
                    values_list.append("NULL")
                elif isinstance(value, str):  # Escape single quotes in strings
                    values_list.append("'" + value.replace("'", "\\'") + "'")
                else:  # Convert other types to strings
                    values_list.append(str(value))
            
            # Join the values into a string for the SQL query
            values = ', '.join(values_list)

            #print("handling value: " + str(values))
            
            # Construct the SQL query using regular string concatenation
            query1 = "INSERT INTO " + table1 + " (batch_id, " + ', '.join(df1.columns) + ") " + \
                    "VALUES (" + values + ")"
            
            #print(query1)
            
            # Execute the query directly
            self.executeSQL(query1)




        
        # Insert records from df2 into Bifrost_staking_table table
        for _, row in df2.iterrows():
            # Safely format the values as a comma-separated string
            values_list = [
                "NULL" if pd.isna(value) else
                "'" + str(value).replace("'", "\\'") + "'" if isinstance(value, str) else
                str(value)
                for value in [batch_id] + row.tolist()
            ]
            values = ', '.join(values_list)

            # Create the query
            query2 = (
                "INSERT INTO " + table2 + " (batch_id, " + ", ".join(df2.columns) + ") "
                "VALUES (" + values + ");"
            )

            # Execute the query
            self.executeSQL(query2)
        
        # insert into df3 for the batch ID
        query3 = "INSERT INTO " + table3 + " (batch_id, chain, " + 'status' + ") " + \
                    "VALUES (\"" + str(batch_id) + "\", \"Bifrost\",\"F\""  + ")"
        
        #print(query3)
        
        self.executeSQL(query3)


        
        print(f"Records successfully updated for batch_id {batch_id}.")
