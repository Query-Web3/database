"""
Comprehensive tests for SQL_DB_hydration.py module.

Tests Hydration database operations including table initialization,
data insertion, and error handling.
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

os.environ['DB_USERNAME'] = 'test_user'
os.environ['DB_PASSWORD'] = 'test_pass'
os.environ['DB_NAME'] = 'test_db'

from SQL_DB_hydration import SQL_DB_Hydration


class TestSQLDBHydrationInit(unittest.TestCase):
    """Test SQL_DB_Hydration initialization."""
    
    @patch('mysql.connector.connect')
    def test_init_basic(self, mock_connect):
        """Test basic initialization."""
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db',
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
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db',
            initializeTable=True
        )
        
        # Should have executed CREATE TABLE statement
        self.assertTrue(mock_cursor.execute.called)
        create_table_call = str(mock_cursor.execute.call_args_list[0])
        self.assertIn('CREATE TABLE', create_table_call)
        self.assertIn('hydration_data', create_table_call)


class TestSQLDBHydrationTableCreation(unittest.TestCase):
    """Test table creation in SQL_DB_Hydration."""
    
    @patch('mysql.connector.connect')
    def test_initialize_tables(self, mock_connect):
        """Test hydration_data table creation."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        db.initialize_tables()
        
        # Verify table creation SQL
        call_args = str(mock_cursor.execute.call_args)
        self.assertIn('CREATE TABLE IF NOT EXISTS hydration_data', call_args)
        self.assertIn('batch_id', call_args)
        self.assertIn('asset_id', call_args)
        self.assertIn('symbol', call_args)
        self.assertIn('farm_apr', call_args)
        self.assertIn('pool_apr', call_args)
        self.assertIn('total_apr', call_args)


class TestSQLDBHydrationExecution(unittest.TestCase):
    """Test SQL execution in SQL_DB_Hydration."""
    
    @patch('mysql.connector.connect')
    def test_execute_sql_basic(self, mock_connect):
        """Test basic SQL execution."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'test')]
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        result = db.executeSQL("SELECT * FROM test")
        
        self.assertEqual(result, [(1, 'test')])
        mock_cursor.execute.assert_called_with("SELECT * FROM test")
    
    @patch('mysql.connector.connect')
    def test_execute_sql_with_params(self, mock_connect):
        """Test parameterized SQL execution."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        params = ('HDX', 10.5)
        result = db.executeSQL(
            "INSERT INTO test VALUES (%s, %s)",
            params=params
        )
        
        mock_cursor.execute.assert_called_with(
            "INSERT INTO test VALUES (%s, %s)",
            params
        )


class TestSQLDBHydrationUpdate(unittest.TestCase):
    """Test Hydration database update operations."""
    
    @patch('mysql.connector.connect')
    def test_update_hydration_database(self, mock_connect):
        """Test updating hydration database with processed data."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        processed_data = [
            {
                'asset_id': '1',
                'symbol': 'HDX',
                'farm_apr': 10.5,
                'pool_apr': 4.56,
                'total_apr': 15.06,
                'tvl_usd': 10000.0,
                'volume_usd': 500.0,
                'timestamp': '2024-01-01T00:00:00'
            },
            {
                'asset_id': '2',
                'symbol': 'DOT',
                'farm_apr': 8.25,
                'pool_apr': 3.5,
                'total_apr': 11.75,
                'tvl_usd': 50000.0,
                'volume_usd': 2000.0,
                'timestamp': '2024-01-01T00:00:00'
            }
        ]
        
        batch_id = 123456
        
        db.update_hydration_database(processed_data, batch_id)
        
        # Should have 2 INSERT calls (one per record)
        self.assertEqual(mock_cursor.execute.call_count, 2)
        
        # Verify batch_id is in the calls
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        for call_str in calls:
            self.assertIn(str(batch_id), call_str)
    
    @patch('mysql.connector.connect')
    def test_update_hydration_database_empty(self, mock_connect):
        """Test updating with empty data list."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        processed_data = []
        batch_id = 123456
        
        db.update_hydration_database(processed_data, batch_id)
        
        # Should not execute any SQL for empty data
        self.assertEqual(mock_cursor.execute.call_count, 0)
    
    @patch('mysql.connector.connect')
    def test_update_hydration_database_with_nulls(self, mock_connect):
        """Test updating with NULL/NaN values."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        processed_data = [
            {
                'asset_id': '1',
                'symbol': 'HDX',
                'farm_apr': None,  # NULL value
                'pool_apr': pd.NA,  # pandas NA
                'total_apr': 0.0,
                'tvl_usd': 0.0,
                'volume_usd': 0.0,
                'timestamp': '2024-01-01T00:00:00'
            }
        ]
        
        batch_id = 123456
        
        # Should not raise error with NULL values
        db.update_hydration_database(processed_data, batch_id)
        
        self.assertTrue(mock_cursor.execute.called)
        
        # Verify NULL is in the SQL
        call_str = str(mock_cursor.execute.call_args)
        self.assertIn('NULL', call_str)
    
    @patch('mysql.connector.connect')
    def test_update_hydration_database_special_chars(self, mock_connect):
        """Test updating with special characters in strings."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        processed_data = [
            {
                'asset_id': "1",
                'symbol': "DOT'TEST",  # Single quote
                'farm_apr': 10.0,
                'pool_apr': 5.0,
                'total_apr': 15.0,
                'tvl_usd': 1000.0,
                'volume_usd': 100.0,
                'timestamp': '2024-01-01T00:00:00'
            }
        ]
        
        batch_id = 123456
        
        # Should escape quotes properly
        db.update_hydration_database(processed_data, batch_id)
        
        self.assertTrue(mock_cursor.execute.called)


class TestSQLDBHydrationErrorHandling(unittest.TestCase):
    """Test error handling in SQL_DB_Hydration."""
    
    @patch('mysql.connector.connect')
    def test_connection_error(self, mock_connect):
        """Test handling of connection errors."""
        import mysql.connector
        
        mock_connect.side_effect = mysql.connector.Error("Connection failed")
        
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        with self.assertRaises(mysql.connector.Error):
            db.executeSQL("SELECT 1")
    
    @patch('mysql.connector.connect')
    def test_error_message_method(self, mock_connect):
        """Test errorMessage method."""
        db = SQL_DB_Hydration(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        # Should not raise error
        db.errorMessage("Test error message")


if __name__ == '__main__':
    unittest.main()
