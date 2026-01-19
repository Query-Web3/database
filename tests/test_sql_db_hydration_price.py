"""
Comprehensive tests for SQL_DB_hydration_price.py module.

Tests database interaction for hydration price data.
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

from SQL_DB_hydration_price import SQL_DB_Hydration_Price


class TestSQLDBHydrationPrice(unittest.TestCase):
    """Test SQL_DB_Hydration_Price class."""
    
    @patch('mysql.connector.connect')
    def test_init(self, mock_connect):
        """Test initialization."""
        db = SQL_DB_Hydration_Price(
            userName='u', passWord='p', host='h', dataBase='d'
        )
        self.assertEqual(db.dataBase, 'd')
    
    @patch('mysql.connector.connect')
    def test_initialize_tables(self, mock_connect):
        """Test table initialization."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_Hydration_Price(userName='u', passWord='p', host='h', dataBase='d')
        db.initialize_tables()
        
        self.assertTrue(mock_cursor.execute.called)
        self.assertIn('Hydration_price', str(mock_cursor.execute.call_args))

    @patch('mysql.connector.connect')
    def test_update_hydration_prices_empty(self, mock_connect):
        db = SQL_DB_Hydration_Price(userName='u', passWord='p', host='h', dataBase='d')
        db.update_hydration_prices([], 123) # Should return early

    @patch('mysql.connector.connect')
    def test_execute_sql_error(self, mock_connect):
        import mysql.connector
        mock_connect.side_effect = mysql.connector.Error(errno=1045) # Access denied
        db = SQL_DB_Hydration_Price(userName='u', passWord='p', host='h', dataBase='d')
        with self.assertRaises(mysql.connector.Error):
            db.executeSQL("SELECT 1")


if __name__ == '__main__':
    unittest.main()
