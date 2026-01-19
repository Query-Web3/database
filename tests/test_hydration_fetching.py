"""
Standardized tests for Hydration_Data_fetching.py.
"""

import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
import os
import pandas as pd
import json

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

import Hydration_Data_fetching


class TestHydrationFetching(unittest.TestCase):
    """Test Hydration data fetching functions."""
    
    @patch('Hydration_Data_fetching.subprocess.run')
    @patch('builtins.open', new_callable=mock_open, read_data='{"1": 10.5}')
    def test_fetch_farm_apr_success(self, mock_file, mock_run):
        res = Hydration_Data_fetching.fetch_farm_apr()
        self.assertEqual(res['1'], 10.5)

    @patch('Hydration_Data_fetching.requests.get')
    def test_fetch_tvl_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"tvl_usd": 1000000}]
        mock_get.return_value = mock_response
        
        res = Hydration_Data_fetching.fetch_tvl("1")
        self.assertEqual(res, 1000000.0)

    @patch('Hydration_Data_fetching.requests.get')
    def test_fetch_latest_volume_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"volume_usd": 50000}]
        mock_get.return_value = mock_response
        
        res = Hydration_Data_fetching.fetch_latest_volume("1")
        self.assertEqual(res, 50000.0)

    def test_calculate_pool_apr(self):
        res = Hydration_Data_fetching.calculate_pool_apr(1000000, 100000)
        self.assertGreater(res, 0)

    @patch('Hydration_Data_fetching.fetch_tvl', return_value=1000000)
    @patch('Hydration_Data_fetching.fetch_latest_volume', return_value=50000)
    def test_process_data(self, mock_vol, mock_tvl):
        assets = [{'ID': 1, 'Symbol': 'HDX'}]
        farm_apr = {'1': 10.0}
        
        res = Hydration_Data_fetching.process_data(assets, farm_apr)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['symbol'], 'HDX')
        self.assertEqual(res[0]['tvl_usd'], 1000000.0)


if __name__ == '__main__':
    unittest.main()
