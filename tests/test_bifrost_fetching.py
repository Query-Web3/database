"""
Comprehensive tests for Bifrost_Data_fetching.py module.

Tests data fetching from Bifrost APIs and database updates.
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

import Bifrost_Data_fetching


class TestBifrostDataFetching(unittest.TestCase):
    """Test Bifrost data fetching functions."""
    
    @patch('Bifrost_Data_fetching.requests.get')
    def test_fetch_data_success(self, mock_get):
        """Test successful data fetch from first API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "DOT": {"apy": 0.15},
            "KSM": 0.20
        }
        mock_get.return_value = mock_response
        
        df = Bifrost_Data_fetching.fetch_data()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
    
    @patch('Bifrost_Data_fetching.requests.get')
    def test_fetch_data2_success(self, mock_get):
        """Test successful data fetch from second API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "supportedAssets": [
                {"symbol": "vDOT", "apr": 0.12},
                {"symbol": "vKSM", "apr": 0.18}
            ]
        }
        mock_get.return_value = mock_response
        
        df = Bifrost_Data_fetching.fetch_data2()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]['symbol'], 'vDOT')

    def test_sanitize_df(self):
        """Test DataFrame sanitization."""
        df = pd.DataFrame({
            'col1': [1, float('inf'), 'NaN'],
            'col2': ['a', 'b', 'c']
        })
        
        sanitized = Bifrost_Data_fetching.sanitize_df(df)
        
        self.assertTrue(pd.isna(sanitized.iloc[1]['col1']))
        self.assertTrue(pd.isna(sanitized.iloc[2]['col1']))


class TestMain(unittest.TestCase):
    """Test main execution logic."""
    
    @patch('Bifrost_Data_fetching.SQL_DB')
    @patch('Bifrost_Data_fetching.fetch_data')
    @patch('Bifrost_Data_fetching.fetch_data2')
    @patch('Bifrost_Data_fetching.time.sleep', return_value=None)
    def test_main_loop_iteration(self, mock_sleep, mock_fetch2, mock_fetch1, mock_sql_db):
        """Test one iteration of the main loop."""
        mock_fetch1.return_value = pd.DataFrame({'Asset': ['DOT']})
        mock_fetch2.return_value = pd.DataFrame({'symbol': ['vDOT']})
        
        mock_db_instance = MagicMock()
        mock_sql_db.return_value = mock_db_instance
        
        mock_sleep.side_effect = KeyboardInterrupt()
        
        try:
            Bifrost_Data_fetching.main()
        except KeyboardInterrupt:
            pass
            
        self.assertTrue(mock_db_instance.update_bifrost_database.called)


if __name__ == '__main__':
    unittest.main()
