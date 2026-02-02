"""
Comprehensive tests for SQL_DB_stella.py module.

Tests Stella pool database operations including table initialization,
pool data insertion, and error handling.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import pandas as pd

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

os.environ.setdefault('DB_USERNAME', 'test_user')
os.environ.setdefault('DB_PASSWORD', 'test_pass')
os.environ.setdefault('DB_NAME', 'test_db')

from SQL_DB_stella import SQL_DB_Stella


class TestSQLDBStellaInit(unittest.TestCase):
    """Test SQL_DB_Stella initialization."""
    
    @patch('mysql.connector.connect')
    def test_init_basic(self, mock_connect):
        """Test basic initialization."""
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost',
            db_port=3306
        )
        
        self.assertEqual(db.userName, 'user')
        self.assertEqual(db.passWord, 'pass')
        self.assertEqual(db.dataBase, 'db')
        self.assertEqual(db.host, 'localhost')
        self.assertEqual(db.port, 3306)
    
    @patch('mysql.connector.connect')
    def test_init_with_table_creation(self, mock_connect):
        """Test initialization with table creation."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost',
            initializeTable=True
        )
        
        # Should have executed CREATE TABLE statement
        self.assertTrue(mock_cursor.execute.called)
        create_table_call = str(mock_cursor.execute.call_args_list[0])
        self.assertIn('CREATE TABLE', create_table_call)
        self.assertIn('pool_data', create_table_call)


class TestSQLDBStellaTableCreation(unittest.TestCase):
    """Test table creation in SQL_DB_Stella."""
    
    @patch('mysql.connector.connect')
    def test_initialize_tables(self, mock_connect):
        """Test pool_data table creation."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        db.initialize_tables()
        
        # Verify table creation SQL
        call_args = str(mock_cursor.execute.call_args)
        self.assertIn('CREATE TABLE IF NOT EXISTS pool_data', call_args)
        self.assertIn('pool_id', call_args)
        self.assertIn('token0_symbol', call_args)
        self.assertIn('token1_symbol', call_args)
        self.assertIn('liquidity', call_args)
        self.assertIn('pools_apr', call_args)
        self.assertIn('farming_apr', call_args)


class TestSQLDBStellaUpdate(unittest.TestCase):
    """Test Stella pool database update operations."""
    
    @patch('mysql.connector.connect')
    def test_update_pool_database(self, mock_connect):
        """Test updating pool database with processed data."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        processed_data = [
            {
                'pool_id': 'pool1',
                'token0_id': 'token0',
                'token0_symbol': 'GLMR',
                'token0_name': 'Moonbeam',
                'token0_decimals': 18,
                'token1_id': 'token1',
                'token1_symbol': 'USDC',
                'token1_name': 'USD Coin',
                'token1_decimals': 6,
                'liquidity': 1000000.0,
                'sqrt_price': 1.5,
                'tick': 100,
                'volume_usd_current': 50000.0,
                'volume_usd_24h_ago': 45000.0,
                'volume_usd_24h': 5000.0,
                'tx_count': 150,
                'fees_usd_current': 500.0,
                'fees_usd_24h_ago': 450.0,
                'fees_usd_24h': 50.0,
                'amount_token0': 10000.0,
                'amount_token1': 15000.0,
                'pools_apr': 8.5,
                'farming_apr': 12.0,
                'final_apr': 20.5,
                'token_rewards': '{"STELLA": 100}',
                'timestamp': '2024-01-01T00:00:00'
            }
        ]
        
        batch_id = 123456
        
        db.update_pool_database(processed_data, batch_id)
        
        # Should have 1 INSERT call
        self.assertEqual(mock_cursor.execute.call_count, 1)
        
        # Verify batch_id and data are in the call
        call_str = str(mock_cursor.execute.call_args)
        self.assertIn(str(batch_id), call_str)
        self.assertIn('pool1', call_str)
    
    @patch('mysql.connector.connect')
    def test_update_pool_database_multiple_pools(self, mock_connect):
        """Test updating with multiple pool records."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        processed_data = [
            {
                'pool_id': f'pool{i}',
                'token0_id': f'token0_{i}',
                'token0_symbol': 'GLMR',
                'token0_name': 'Moonbeam',
                'token0_decimals': 18,
                'token1_id': f'token1_{i}',
                'token1_symbol': 'USDC',
                'token1_name': 'USD Coin',
                'token1_decimals': 6,
                'liquidity': 1000000.0 * i,
                'sqrt_price': 1.5,
                'tick': 100,
                'volume_usd_current': 50000.0,
                'volume_usd_24h_ago': 45000.0,
                'volume_usd_24h': 5000.0,
                'tx_count': 150,
                'fees_usd_current': 500.0,
                'fees_usd_24h_ago': 450.0,
                'fees_usd_24h': 50.0,
                'amount_token0': 10000.0,
                'amount_token1': 15000.0,
                'pools_apr': 8.5,
                'farming_apr': 12.0,
                'final_apr': 20.5,
                'token_rewards': '{}',
                'timestamp': '2024-01-01T00:00:00'
            }
            for i in range(1, 4)
        ]
        
        batch_id = 123456
        
        db.update_pool_database(processed_data, batch_id)
        
        # Should have 3 INSERT calls
        self.assertEqual(mock_cursor.execute.call_count, 3)
    
    @patch('mysql.connector.connect')
    def test_update_pool_database_empty(self, mock_connect):
        """Test updating with empty data list."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        processed_data = []
        batch_id = 123456
        
        db.update_pool_database(processed_data, batch_id)
        
        # Should not execute any SQL for empty data
        self.assertEqual(mock_cursor.execute.call_count, 0)
    
    @patch('mysql.connector.connect')
    def test_update_pool_database_with_nulls(self, mock_connect):
        """Test updating with NULL values."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        processed_data = [
            {
                'pool_id': 'pool1',
                'token0_id': None,
                'token0_symbol': 'GLMR',
                'token0_name': None,
                'token0_decimals': 18,
                'token1_id': 'token1',
                'token1_symbol': 'USDC',
                'token1_name': 'USD Coin',
                'token1_decimals': 6,
                'liquidity': pd.NA,
                'sqrt_price': None,
                'tick': 100,
                'volume_usd_current': 0.0,
                'volume_usd_24h_ago': 0.0,
                'volume_usd_24h': 0.0,
                'tx_count': 0,
                'fees_usd_current': None,
                'fees_usd_24h_ago': None,
                'fees_usd_24h': None,
                'amount_token0': 0.0,
                'amount_token1': 0.0,
                'pools_apr': None,
                'farming_apr': None,
                'final_apr': None,
                'token_rewards': None,
                'timestamp': '2024-01-01T00:00:00'
            }
        ]
        
        batch_id = 123456
        
        # Should handle NULL values without error
        db.update_pool_database(processed_data, batch_id)
        
        self.assertTrue(mock_cursor.execute.called)
        
        # Verify NULL is in the SQL
        call_str = str(mock_cursor.execute.call_args)
        self.assertIn('NULL', call_str)
    
    @patch('mysql.connector.connect')
    def test_update_pool_database_json_rewards(self, mock_connect):
        """Test updating with JSON token rewards."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        processed_data = [
            {
                'pool_id': 'pool1',
                'token0_id': 'token0',
                'token0_symbol': 'GLMR',
                'token0_name': 'Moonbeam',
                'token0_decimals': 18,
                'token1_id': 'token1',
                'token1_symbol': 'USDC',
                'token1_name': 'USD Coin',
                'token1_decimals': 6,
                'liquidity': 1000000.0,
                'sqrt_price': 1.5,
                'tick': 100,
                'volume_usd_current': 50000.0,
                'volume_usd_24h_ago': 45000.0,
                'volume_usd_24h': 5000.0,
                'tx_count': 150,
                'fees_usd_current': 500.0,
                'fees_usd_24h_ago': 450.0,
                'fees_usd_24h': 50.0,
                'amount_token0': 10000.0,
                'amount_token1': 15000.0,
                'pools_apr': 8.5,
                'farming_apr': 12.0,
                'final_apr': 20.5,
                'token_rewards': '{"STELLA": 100, "GLMR": 50}',
                'timestamp': '2024-01-01T00:00:00'
            }
        ]
        
        batch_id = 123456
        
        db.update_pool_database(processed_data, batch_id)
        
        self.assertTrue(mock_cursor.execute.called)


class TestSQLDBStellaErrorHandling(unittest.TestCase):
    """Test error handling in SQL_DB_Stella."""
    
    @patch('mysql.connector.connect')
    def test_connection_error(self, mock_connect):
        """Test handling of connection errors."""
        import mysql.connector
        
        mock_connect.side_effect = mysql.connector.Error("Connection failed")
        
        db = SQL_DB_Stella(
            userName='user',
            passWord='pass',
            dataBase='db',
            host='localhost'
        )
        
        with self.assertRaises(mysql.connector.Error):
            db.executeSQL("SELECT 1")


if __name__ == '__main__':
    unittest.main()
