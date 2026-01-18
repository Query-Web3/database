"""
Comprehensive tests for stellaswap_store_raw_data.py module.

Tests Stellaswap data fetching, token amount calculations, and pool processing.
"""

import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import math

# Setup path and environment
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, cao_dir)

os.environ['DB_USERNAME'] = 'test_user'
os.environ['DB_PASSWORD'] = 'test_pass'
os.environ['DB_NAME'] = 'test_db'

import stellaswap_store_raw_data


class TestFetchPoolsAPR(unittest.TestCase):
    """Test fetching pools APR."""
    
    @patch('stellaswap_store_raw_data.requests.get')
    def test_fetch_pools_apr_success(self, mock_get):
        """Test successful pools APR fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "isSuccess": True,
            "result": {
                "pool1": 0.5,
                "pool2": 0.75
            }
        }
        mock_get.return_value = mock_response
        
        result = stellaswap_store_raw_data.fetch_pools_apr()
        
        self.assertEqual(result['pool1'], 0.5)
        self.assertEqual(result['pool2'], 0.75)


class TestFetchFarmingAPR(unittest.TestCase):
    """Test fetching farming APR."""
    
    @patch('stellaswap_store_raw_data.requests.get')
    def test_fetch_farming_apr_success(self, mock_get):
        """Test successful farming APR fetch."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "code": 200,
            "result": {
                "pools": {
                    "pool1": {"apr": 10.0},
                    "pool2": {"apr": 12.5}
                }
            }
        }
        mock_get.return_value = mock_response
        
        result = stellaswap_store_raw_data.fetch_farming_apr()
        
        self.assertEqual(result['pool1']['apr'], 10.0)


class TestCalculateTokenAmounts(unittest.TestCase):
    """Test token amount calculations."""
    
    def test_calculate_token_amounts_in_range(self):
        amount0, amount1 = stellaswap_store_raw_data.calculate_token_amounts(
            liquidity=1000,
            sqrt_price_current=25,
            sqrt_price_low=20,
            sqrt_price_high=30
        )
        self.assertGreater(amount0, 0)
        self.assertGreater(amount1, 0)


class TestFetchTokenAmounts(unittest.TestCase):
    """Test fetching token amounts via The Graph."""
    
    @patch('stellaswap_store_raw_data.requests.post')
    def test_fetch_token_amounts_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": {
                "positions": [
                    {
                        "tickLower": {"tickIdx": "1000"},
                        "tickUpper": {"tickIdx": "2000"},
                        "liquidity": "1000000",
                        "pool": {
                            "sqrtPrice": str(int(25 * (2**96))),
                            "token0": {"decimals": "18"},
                            "token1": {"decimals": "18"}
                        }
                    }
                ]
            }
        }
        mock_post.return_value = mock_response
        
        a0, a1 = stellaswap_store_raw_data.fetch_token_amounts("pool1")
        self.assertIsInstance(a0, float)
        self.assertIsInstance(a1, float)


class TestProcessDataCompleteness(unittest.TestCase):
    """Test process_data with full fields."""
    
    @patch('stellaswap_store_raw_data.fetch_token_amounts', return_value=(10.0, 20.0))
    def test_process_data_with_hour_data(self, mock_fetch):
        raw_data = {
            "data": {
                "pools": [
                    {
                        "id": "pool1",
                        "token0": {"id": "t0", "symbol": "S0", "name": "N0", "decimals": "18"},
                        "token1": {"id": "t1", "symbol": "S1", "name": "N1", "decimals": "18"},
                        "liquidity": "100",
                        "sqrtPrice": "100",
                        "tick": "10",
                        "volumeUSD": "1000",
                        "txCount": "5",
                        "feesUSD": "10",
                        "poolHourData": [{"feesUSD": "8", "volumeUSD": "800"}]
                    }
                ]
            }
        }
        result = stellaswap_store_raw_data.process_data(raw_data, {"pool1": 5.0}, {"pool1": {"apr": 10.0}})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['fees_usd_24h'], 2.0)
        self.assertEqual(result[0]['volume_usd_24h'], 200.0)


class TestStellaswapMain(unittest.TestCase):
    """Test stellaswap main loop."""
    
    @patch('stellaswap_store_raw_data.SQL_DB_Stella')
    @patch('stellaswap_store_raw_data.fetch_pools_apr', return_value={})
    @patch('stellaswap_store_raw_data.fetch_farming_apr', return_value={})
    @patch('stellaswap_store_raw_data.fetch_pool_data', return_value={"data": {"pools": []}})
    @patch('stellaswap_store_raw_data.time.sleep', side_effect=KeyboardInterrupt)
    def test_main_iteration(self, mock_sleep, mock_fetch1, mock_fetch2, mock_fetch3, mock_sql):
        try:
            stellaswap_store_raw_data.main()
        except KeyboardInterrupt:
            pass
        self.assertTrue(mock_sql.called)


if __name__ == '__main__':
    unittest.main()
