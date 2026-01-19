"""
Comprehensive tests for SQL_DB_mergeTables.py module.

Tests table merging operations, JSON serialization, data sanitization,
and payload insertion.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import pandas as pd
import numpy as np
import decimal
import datetime

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

os.environ['DB_USERNAME'] = 'test_user'
os.environ['DB_PASSWORD'] = 'test_pass'
os.environ['DB_NAME'] = 'test_db'

from SQL_DB_mergeTables import SQL_DB_MergeTables


class TestSQLDBMergeTablesInit(unittest.TestCase):
    """Test SQL_DB_MergeTables initialization."""
    
    @patch('mysql.connector.connect')
    def test_init_basic(self, mock_connect):
        """Test basic initialization."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db',
            port=3306
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
        mock_cursor.fetchall.return_value = []
        
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db',
            initializeTable=True
        )
        
        # Should have executed CREATE TABLE statement
        self.assertTrue(mock_cursor.execute.called)


class TestSanitization(unittest.TestCase):
    """Test data sanitization functions."""
    
    @patch('mysql.connector.connect')
    def test_sanitize_scalar_float(self, mock_connect):
        """Test sanitizing float values."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        # Valid float
        self.assertEqual(db._sanitize_scalar(5.5), 5.5)
        
        # Infinity should be None
        self.assertIsNone(db._sanitize_scalar(float('inf')))
        self.assertIsNone(db._sanitize_scalar(float('-inf')))
        
        # NaN should be converted
        result = db._sanitize_scalar(float('nan'))
        self.assertTrue(result is None or (isinstance(result, float) and np.isnan(result)))
    
    @patch('mysql.connector.connect')
    def test_sanitize_scalar_decimal(self, mock_connect):
        """Test sanitizing Decimal values."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        # Valid decimal
        result = db._sanitize_scalar(decimal.Decimal('10.5'))
        self.assertEqual(result, 10.5)
        self.assertIsInstance(result, float)
    
    @patch('mysql.connector.connect')
    def test_sanitize_scalar_int(self, mock_connect):
        """Test sanitizing int values."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        self.assertEqual(db._sanitize_scalar(42), 42)
        self.assertEqual(db._sanitize_scalar(np.int64(100)), 100)
    
    @patch('mysql.connector.connect')
    def test_sanitize_scalar_none(self, mock_connect):
        """Test sanitizing None values."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        self.assertIsNone(db._sanitize_scalar(None))
    
    @patch('mysql.connector.connect')
    def test_deep_clean_dict(self, mock_connect):
        """Test deep cleaning of dictionaries."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        dirty_dict = {
            'a': float('inf'),
            'b': 5.5,
            'c': None,
            'd': {'nested': float('-inf')}
        }
        
        clean = db._deep_clean(dirty_dict)
        
        self.assertIsNone(clean['a'])
        self.assertEqual(clean['b'], 5.5)
        self.assertIsNone(clean['c'])
        self.assertIsNone(clean['d']['nested'])
    
    @patch('mysql.connector.connect')
    def test_deep_clean_list(self, mock_connect):
        """Test deep cleaning of lists."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        dirty_list = [1, 2.5, float('inf'), None, [float('-inf'), 10]]
        
        clean = db._deep_clean(dirty_list)
        
        self.assertEqual(clean[0], 1)
        self.assertEqual(clean[1], 2.5)
        self.assertIsNone(clean[2])
        self.assertIsNone(clean[3])
        self.assertIsNone(clean[4][0])
        self.assertEqual(clean[4][1], 10)


class TestDataFrameConversion(unittest.TestCase):
    """Test DataFrame to JSON conversion."""
    
    @patch('mysql.connector.connect')
    def test_df_to_json_array(self, mock_connect):
        """Test converting DataFrame to JSON array."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [1.5, 2.5, 3.5]
        })
        
        result = db._df_to_json_array(df)
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['col1'], 1)
        self.assertEqual(result[0]['col2'], 'a')
        self.assertEqual(result[0]['col3'], 1.5)
    
    @patch('mysql.connector.connect')
    def test_df_to_json_array_with_nan(self, mock_connect):
        """Test converting DataFrame with NaN values."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        df = pd.DataFrame({
            'col1': [1, np.nan, 3],
            'col2': [np.inf, 2.5, -np.inf]
        })
        
        result = db._df_to_json_array(df)
        
        self.assertEqual(len(result), 3)
        self.assertIsNone(result[1]['col1'])  # NaN -> None
        self.assertIsNone(result[0]['col2'])  # Inf -> None
    
    @patch('mysql.connector.connect')
    def test_df_to_json_array_empty(self, mock_connect):
        """Test converting empty DataFrame."""
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        df = pd.DataFrame()
        result = db._df_to_json_array(df)
        
        self.assertEqual(result, [])


class TestJSONSerialization(unittest.TestCase):
    """Test JSON serialization helpers."""
    
    def test_json_default_datetime(self):
        """Test JSON default handler for datetime."""
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        result = SQL_DB_MergeTables._json_default(dt)
        self.assertEqual(result, '2024-01-01T12:00:00')
    
    def test_json_default_decimal(self):
        """Test JSON default handler for Decimal."""
        dec = decimal.Decimal('10.5')
        result = SQL_DB_MergeTables._json_default(dec)
        self.assertEqual(result, 10.5)
    
    def test_json_default_other(self):
        """Test JSON default handler for other types."""
        result = SQL_DB_MergeTables._json_default("string")
        self.assertEqual(result, "string")


class TestDatabaseQueries(unittest.TestCase):
    """Test database query methods."""
    
    @patch('mysql.connector.connect')
    def test_fetch_df(self, mock_connect):
        """Test fetching DataFrame from query."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, 'a'), (2, 'b')]
        mock_cursor.description = [('id',), ('name',)]
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        df = db.fetch_df("SELECT * FROM test")
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), ['id', 'name'])
    
    @patch('mysql.connector.connect')
    def test_fetch_one(self, mock_connect):
        """Test fetching single row."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (123, datetime.datetime(2024, 1, 1))
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        result = db.fetch_one("SELECT * FROM test LIMIT 1")
        
        self.assertEqual(result[0], 123)
        self.assertIsInstance(result[1], datetime.datetime)
    
    @patch('mysql.connector.connect')
    def test_execute_sql(self, mock_connect):
        """Test SQL execution."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        db.executeSQL("INSERT INTO test VALUES (1, 'data')")
        
        mock_cursor.execute.assert_called()
        mock_connect.return_value.commit.assert_called()


class TestPayloadInsertion(unittest.TestCase):
    """Test payload insertion."""
    
    @patch('mysql.connector.connect')
    def test_insert_combined_payload(self, mock_connect):
        """Test inserting combined payload."""
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        payload = {
            'bifrost_data': [{'asset': 'DOT', 'price': 5.0}],
            'hydration_data': [{'symbol': 'HDX', 'apr': 10.0}]
        }
        
        db.insert_combined_payload(payload)
        
        # Should have executed INSERT
        self.assertTrue(mock_cursor.execute.called)
        call_str = str(mock_cursor.execute.call_args)
        self.assertIn('INSERT INTO multipleFACT', call_str)
        self.assertIn('payload', call_str)


class TestRunMerge(unittest.TestCase):
    """Test the main run_merge workflow."""
    
    @patch('mysql.connector.connect')
    def test_run_merge(self, mock_connect):
        """Test run_merge method."""
        mock_cursor = MagicMock()
        
        # Setup mock responses for different queries
        def execute_side_effect(query, params=None):
            # Return empty for most queries
            return None
        
        mock_cursor.execute.side_effect = execute_side_effect
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = (123456, datetime.datetime(2024, 1, 1))
        mock_cursor.description = None
        
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db = SQL_DB_MergeTables(
            userName='user',
            passWord='pass',
            host='localhost',
            dataBase='db'
        )
        
        # Should not raise error
        db.run_merge()
        
        # Should have executed multiple queries
        self.assertGreater(mock_cursor.execute.call_count, 0)


if __name__ == '__main__':
    unittest.main()
