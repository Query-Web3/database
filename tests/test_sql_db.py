"""
Comprehensive tests for SQL_DB.py module.

Tests database connection, query execution, error handling,
and Bifrost database update operations.
"""

import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
import os
import pandas as pd
import numpy as np
import math

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

os.environ.setdefault('DB_USERNAME', 'test_user')
os.environ.setdefault('DB_PASSWORD', 'test_pass')
os.environ.setdefault('DB_NAME', 'test_db')
os.environ['DB_PORT'] = '3306'

import SQL_DB


class TestSQLDBInit(unittest.TestCase):
    """Test SQL_DB initialization."""
    
    @patch('mysql.connector.connect')
    def test_init_with_credentials(self, mock_connect):
        """Test initialization with individual credentials."""
        db = SQL_DB.SQL_DB(
            userName='user1',
            passWord='pass1',
            dataBase='db1',
            host='localhost',
            port=3306
        )
        
        self.assertEqual(db.userName, 'user1')
        self.assertEqual(db.passWord, 'pass1')
        self.assertEqual(db.dataBase, 'db1')
        self.assertEqual(db.host, 'localhost')
        self.assertEqual(db.port, 3306)
    
    @patch('builtins.open', new_callable=mock_open, read_data='{"user": "config_user", "pass": "config_pass", "database": "config_db"}')
    @patch('mysql.connector.connect')
    def test_init_with_db_config(self, mock_connect, mock_file):
        """Test initialization with db_config file."""
        db = SQL_DB.SQL_DB(db_config='dummy_config.json')
        
        self.assertEqual(db.userName, 'config_user')
        self.assertEqual(db.passWord, 'config_pass')
        self.assertEqual(db.dataBase, 'config_db')
    
    @patch('mysql.connector.connect')
    def test_init_table_creation(self, mock_connect):
        """Test table initialization during init."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB.SQL_DB(
            userName='user',
            passWord='pass',
            dataBase='db',
            initializeTable=True
        )
        
        # Should have executed CREATE TABLE statements
        self.assertTrue(mock_cursor.execute.called)
        calls = mock_cursor.execute.call_args_list
        
        # Check that table creation SQL was executed
        create_table_calls = [c for c in calls if 'CREATE TABLE' in str(c)]
        self.assertGreater(len(create_table_calls), 0)


class TestSQLDBExecution(unittest.TestCase):
    """Test SQL query execution."""
    
    @patch('mysql.connector.connect')
    def test_execute_sql_select(self, mock_connect):
        """Test SELECT query execution."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'data')]
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        result = db.executeSQL("SELECT * FROM test_table")
        
        self.assertEqual(result, [(1, 'test'), (2, 'data')])
        mock_cursor.execute.assert_called_with("SELECT * FROM test_table")
        mock_connect.return_value.commit.assert_called()
    
    @patch('mysql.connector.connect')
    def test_execute_sql_with_params(self, mock_connect):
        """Test parameterized query execution."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'test')]
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        result = db.executeSQL(
            "SELECT * FROM test WHERE id = %s",
            params=(1,)
        )
        
        self.assertEqual(result, [(1, 'test')])
        mock_cursor.execute.assert_called_with(
            "SELECT * FROM test WHERE id = %s",
            (1,)
        )
    
    @patch('mysql.connector.connect')
    def test_execute_sql_insert(self, mock_connect):
        """Test INSERT query execution."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = Exception("No results for INSERT")
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        result = db.executeSQL("INSERT INTO test VALUES (1, 'data')")
        
        # INSERT returns None
        self.assertIsNone(result)
        mock_connect.return_value.commit.assert_called()


class TestSQLDBErrorHandling(unittest.TestCase):
    """Test error handling in SQL_DB."""
    
    @patch('mysql.connector.connect')
    def test_connection_error_access_denied(self, mock_connect):
        """Test handling of access denied error."""
        import mysql.connector
        from mysql.connector import errorcode
        
        error = mysql.connector.Error()
        error.errno = errorcode.ER_ACCESS_DENIED_ERROR
        mock_connect.side_effect = error
        
        db = SQL_DB.SQL_DB(userName='bad', passWord='bad', dataBase='d')
        
        # executeSQL prints error but doesn't re-raise in the code if it catches mysql.connector.Error
        db.executeSQL("SELECT 1")
    
    @patch('mysql.connector.connect')
    def test_connection_error_bad_db(self, mock_connect):
        """Test handling of bad database error."""
        import mysql.connector
        from mysql.connector import errorcode
        
        error = mysql.connector.Error()
        error.errno = errorcode.ER_BAD_DB_ERROR
        mock_connect.side_effect = error
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='bad_db')
        
        db.executeSQL("SELECT 1")


class TestBifrostDatabaseUpdate(unittest.TestCase):
    """Test Bifrost database update operations."""
    
    @patch('mysql.connector.connect')
    def test_update_bifrost_database(self, mock_connect):
        """Test updating Bifrost database with dataframes."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        
        # Create test dataframes
        df1 = pd.DataFrame({
            'Asset': ['DOT', 'KSM'],
            'price': [5.0, 25.0]
        })
        
        df2 = pd.DataFrame({
            'symbol': ['vDOT'],
            'apr': [15.5]
        })
        
        batch_id = 123456
        
        db.update_bifrost_database(df1, df2, batch_id)
        
        # Should have executed INSERT statements
        self.assertTrue(mock_cursor.execute.called)
    
    @patch('mysql.connector.connect')
    def test_update_bifrost_database_with_nulls(self, mock_connect):
        """Test Bifrost database update with NULL values."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        
        df1 = pd.DataFrame({
            'Asset': ['DOT', None],
            'price': [5.0, np.nan]
        })
        
        df2 = pd.DataFrame({
            'symbol': ['vDOT'],
            'apr': [None]
        })
        
        batch_id = 123456
        
        db.update_bifrost_database(df1, df2, batch_id)
        self.assertTrue(mock_cursor.execute.called)


class TestSQLDBConnectionManagement(unittest.TestCase):
    """Test database connection management."""
    
    @patch('mysql.connector.connect')
    def test_connection_close(self, mock_connect):
        """Test that connections are properly closed."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value = mock_conn
        
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        db.executeSQL("SELECT 1")
        
        # Verify cursor and connection were closed
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()


if __name__ == '__main__':
    unittest.main()
