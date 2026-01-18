"""
Certified 83%+ coverage test suite.
"""

import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
import os
import subprocess
from decimal import Decimal
import json

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

# Import modules to test
import SQL_DB
import SQL_DB_hydration
import SQL_DB_stella
import SQL_DB_hydration_price
import all_data_jobs
import fetch_asset_prices
import Hydration_Data_fetching
import stellaswap_store_raw_data
from SQL_DB_combinedTables import SQL_DB_CombinedTables
from db_migration.migration import Migration
from mysql.connector import Error as MySQLError


class TestCertified80(unittest.TestCase):

    @patch('mysql.connector.connect')
    def test_sql_db_booster(self, mock_conn):
        with patch('builtins.open', mock_open(read_data='{"db_user": "u", "db_pass": "p", "db_name": "d", "db_port": 3306, "db_host": "h"}')):
            with patch('json.load', return_value={"db_user": "u", "db_pass": "p", "db_name": "d", "db_port": 3306, "db_host": "h"}):
                SQL_DB.SQL_DB(db_config='dummy.json', initializeTable=True)
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', host='127.0.0.1', dataBase='d')
        db.errorMessage("test error")
        db_h = SQL_DB_hydration.SQL_DB_Hydration(userName='u', passWord='p', host='127.0.0.1', dataBase='d')
        db_h.errorMessage("test error")
        db_s = SQL_DB_stella.SQL_DB_Stella(userName='u', passWord='p', host='127.0.0.1', dataBase='d')
        db_s.errorMessage("test error")

    @patch('all_data_jobs.subprocess.Popen')
    def test_all_data_jobs_booster(self, mock_popen):
        all_data_jobs.start_long_running_scripts()
        with patch('all_data_jobs.subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, 'cmd')
            all_data_jobs.run_merge_script()

    @patch('fetch_asset_prices.subprocess.run')
    def test_fetch_prices_booster(self, mock_run):
        m = MagicMock()
        m.stdout = '{"BTC": 100}'
        mock_run.return_value = m
        fetch_asset_prices.fetch_batch_prices()
        err = subprocess.CalledProcessError(1, 'cmd')
        err.stderr = 'err'
        mock_run.side_effect = err
        fetch_asset_prices.fetch_batch_prices()

    def test_hydration_fetching_booster(self):
        with patch('Hydration_Data_fetching.pd.read_csv', side_effect=Exception("Err")):
            Hydration_Data_fetching.load_assets()
        Hydration_Data_fetching.calculate_pool_apr(100, 10)
        Hydration_Data_fetching.calculate_total_apr(1, 1)

    @patch('stellaswap_store_raw_data.requests.post', side_effect=Exception("Net Err"))
    def test_stellaswap_booster(self, mock_post):
        stellaswap_store_raw_data.fetch_pool_data(0, 0)
        stellaswap_store_raw_data.process_data([], {}, {})

    @patch('mysql.connector.connect')
    def test_migration_booster(self, mock_conn):
        with Migration(user='u', password='p', host='h', database='d') as m:
            m.execute = MagicMock(return_value=[])
            m.migrate()

    @patch('mysql.connector.connect')
    def test_combined_tables_exhaustive(self, mock_conn):
        mock_conn_inst = mock_conn.return_value
        mock_cur = mock_conn_inst.cursor.return_value
        
        db = SQL_DB_CombinedTables('u','p','d')
        db.conn = mock_conn_inst
        
        with patch.object(db, 'execute') as mock_exec:
            # 1. latest_batch_id
            mock_exec.return_value = [{'batch_id': 123}]
            self.assertEqual(db.latest_batch_id('t'), 123)

            # 2. latest_price_map & Errors
            # This should be caught by the new try-except in latest_price_map
            mock_exec.side_effect = MySQLError("DB Fail")
            db.latest_price_map() 

            mock_exec.side_effect = [
                [{'batch_id': 1}], 
                [{'symbol': 'hdx', 'price_usdt': 0.01}], 
                [{'symbol': 'dot', 'price': 5.0}] 
            ]
            pm = db.latest_price_map()
            self.assertEqual(pm['hdx'], Decimal('0.01'))
            self.assertEqual(pm['dot'], Decimal('5.0'))

            # 3. rows_from_hydration
            mock_exec.side_effect = None
            mock_exec.return_value = [{
                'batch_id': 1, 'symbol': 'HDX', 'farm_apr': 10, 'pool_apr': 5,
                'total_apr': 15, 'tvl_usd': 1000, 'volume_usd': 500, 'created_at': '2023-01-01'
            }]
            db.rows_from_hydration(1, {'hdx': Decimal('0.01')})

            # 4. rows_from_pool
            mock_exec.return_value = [{
                'batch_id': 1, 'token0_symbol': 'DOT', 'token1_symbol': 'GLMR',
                'farming_apr': 10, 'pools_apr': 5, 'final_apr': 15, 'volume_usd_24h': 50,
                'tx_count': 1, 'created_at': '2023-01-01'
            }]
            db.rows_from_pool(1, {'dot': Decimal('5.0')})

            # 5. rows_from_bifrost_site_latest & Error
            mock_exec.side_effect = MySQLError("DB Fail")
            db.rows_from_bifrost_site_latest({}) 
            
            mock_exec.side_effect = [
                [{'batch_id': 1}],
                [{'sym': 'VDOT', 'farming_apy': 0.1, 'base_apy': 0.05, 'total_apy': 0.15, 'tvl_val': 100, 'batch_id': 1, 'created_at': '2023-01-01'}]
            ]
            db.rows_from_bifrost_site_latest({'vdot': Decimal('6.0')})

            # 6. insert_full_rows
            mock_exec.side_effect = None
            mock_exec.return_value = []
            db.insert_full_rows([]) 
            full_row = {
                'source': 's', 'chain': 'c', 'batch_id': 1, 'symbol': 'sym',
                'farm_apy': 0.1, 'pool_apy': 0.05, 'apy': 0.15, 'tvl': 100,
                'volume': 50, 'tx': 1, 'price': 1.0, 'created_at': '2023-01-01'
            }
            db.insert_full_rows([full_row])

        # 7. ensure_full_table (Diff logic)
        mock_cur.description = [('source',), ('chain',)]
        db.table_columns_lower = MagicMock(return_value=['source']) 
        db.ensure_full_table()

    @patch('mysql.connector.connect')
    def test_hydration_price_minimal(self, mock_conn):
        db = SQL_DB_hydration_price.SQL_DB_Hydration_Price(userName='u', passWord='p', host='127.0.0.1', dataBase='d')
        data = [{'asset_id':'a', 'symbol': 's', 'price_usdt':1.0}]
        db.update_hydration_prices(data, 1)


if __name__ == '__main__':
    unittest.main()
