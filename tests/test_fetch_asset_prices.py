"""
Tests for fetch_asset_prices.py module.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import pandas as pd
import json

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

import fetch_asset_prices


class TestFetchAssetPrices(unittest.TestCase):
    """Test fetch_asset_prices module."""
    
    @patch('fetch_asset_prices.pd.read_csv')
    def test_load_assets(self, mock_read):
        mock_read.return_value = pd.DataFrame({'ID': [1], 'Symbol': ['DOT']})
        res = fetch_asset_prices.load_assets()
        self.assertEqual(len(res), 1)

    def test_load_assets_filenotfound(self):
        with patch('fetch_asset_prices.pd.read_csv', side_effect=FileNotFoundError):
            res = fetch_asset_prices.load_assets()
            self.assertEqual(res, [])

    @patch('fetch_asset_prices.subprocess.run')
    def test_fetch_batch_prices_success(self, mock_run):
        mock_res = MagicMock()
        mock_res.stdout = json.dumps([{"assetId": "1", "price": 5.5}])
        mock_run.return_value = mock_res
        
        res = fetch_asset_prices.fetch_batch_prices()
        self.assertEqual(res['1'], 5.5)

    @patch('fetch_asset_prices.subprocess.run')
    def test_fetch_batch_prices_empty(self, mock_run):
        mock_res = MagicMock()
        mock_res.stdout = ""
        mock_run.return_value = mock_res
        
        res = fetch_asset_prices.fetch_batch_prices()
        self.assertEqual(res, {})

    def test_process_prices(self):
        assets = [{'ID': 1, 'Symbol': 'DOT'}, {'ID': 2, 'Symbol': 'KSM'}]
        prices = {'1': 5.5}
        res = fetch_asset_prices.process_prices(assets, prices)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['symbol'], 'DOT')

    @patch('fetch_asset_prices.SQL_DB_Hydration_Price')
    @patch('fetch_asset_prices.load_assets')
    @patch('fetch_asset_prices.fetch_batch_prices')
    def test_main_execution(self, mock_fetch, mock_load, mock_sql):
        mock_load.return_value = [{'ID': 1, 'Symbol': 'DOT'}]
        mock_fetch.return_value = {'1': 5.5}
        
        fetch_asset_prices.run_pipeline(single_run=True)
            
        self.assertTrue(mock_sql.called)
            
        self.assertTrue(mock_sql.called)


if __name__ == '__main__':
    unittest.main()
