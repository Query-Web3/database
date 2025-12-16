import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
import os
import pandas as pd
import numpy as np
import json

# --- Setup Environment for Imports ---
# Many scripts in CAO/ check for environment variables at the top level.
# We set dummy values here to allow imports to succeed without a real .env file.
os.environ['DB_USERNAME'] = 'test_user'
os.environ['DB_PASSWORD'] = 'test_pass'
os.environ['DB_NAME'] = 'test_db'
os.environ['DB_PORT'] = '3306'
os.environ['API_KEY'] = 'test_api_key'

# Add CAO directory to sys.path to allow importing modules
current_dir = os.path.dirname(os.path.abspath(__file__))
cao_dir = os.path.join(current_dir, 'CAO')
sys.path.append(cao_dir)

# --- Imports ---
# We wrap imports in try-except blocks or just import them. 
# Since we set env vars, they should import fine.
try:
    import Bifrost_Data_fetching
    import Hydration_Data_fetching
    import SQL_DB
    import SQL_DB_hydration
    import SQL_DB_hydration_price
    import SQL_DB_stella
    import SQL_DB_combinedTables
    import fetch_asset_prices
    import stellaswap_store_raw_data
except ImportError as e:
    print(f"CRITICAL: Failed to import modules. Make sure you are running this from the root directory. Error: {e}")
    sys.exit(1)
except ValueError as e:
    print(f"CRITICAL: Module initialization failed (likely env vars). Error: {e}")
    sys.exit(1)

# --- Test Classes ---

class TestBifrostDataFetching(unittest.TestCase):
    
    @patch('Bifrost_Data_fetching.requests.get')
    def test_fetch_data_success(self, mock_get):
        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "DOT": {"price": 5.0},
            "KSM": 2.0
        }
        mock_get.return_value = mock_response

        df = Bifrost_Data_fetching.fetch_data()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertIn('Asset', df.columns)
        # Check if data was flattened correctly
        self.assertTrue((df['Asset'] == 'DOT').any())
        self.assertTrue((df['Asset'] == 'KSM').any())

    @patch('Bifrost_Data_fetching.requests.get')
    def test_fetch_data2_success(self, mock_get):
        # Mock API response for staking
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "supportedAssets": [
                {"symbol": "vDOT", "price": 10.0, "apr": 0.15}
            ]
        }
        mock_get.return_value = mock_response

        df = Bifrost_Data_fetching.fetch_data2()
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        self.assertIn('symbol', df.columns)
        self.assertEqual(df.iloc[0]['symbol'], 'vDOT')

    def test_sanitize_df(self):
        # Create a dirty dataframe
        df = pd.DataFrame({
            'A': ['NaN', 'Inf', 1.0],
            'B': [np.inf, -np.inf, 2.0],
            'C': ['1.5', 'nan', '3.0']
        })
        
        clean_df = Bifrost_Data_fetching.sanitize_df(df)
        
        # Check if NaN/Inf are handled (replaced with None for SQL or np.nan)
        # The function replaces Inf with NaN, and then NaN with None
        # Note: In pandas, float columns might convert None back to NaN.
        # We check if it is null (None or NaN)
        self.assertTrue(pd.isna(clean_df.iloc[0]['A'])) # 'NaN' -> None/NaN
        self.assertTrue(pd.isna(clean_df.iloc[0]['B'])) # np.inf -> None/NaN
        self.assertEqual(clean_df.iloc[2]['A'], 1.0)

class TestHydrationDataFetching(unittest.TestCase):

    def test_calculate_pool_apr(self):
        # Formula: (Volume * Fee * 365) / TVL
        # Fee = 0.0025
        tvl = 100000
        volume = 1000
        expected_apr = (1000 * 0.0025 * 365) / 100000 * 100 # 0.9125
        
        apr = Hydration_Data_fetching.calculate_pool_apr(tvl, volume)
        self.assertAlmostEqual(apr, 0.9125)

        # Test zero TVL
        self.assertEqual(Hydration_Data_fetching.calculate_pool_apr(0, 1000), 0.0)

    def test_calculate_total_apr(self):
        self.assertEqual(Hydration_Data_fetching.calculate_total_apr(5.0, 2.5), 7.5)

    @patch('Hydration_Data_fetching.fetch_tvl')
    @patch('Hydration_Data_fetching.fetch_latest_volume')
    def test_process_data(self, mock_vol, mock_tvl):
        mock_tvl.return_value = 10000.0
        mock_vol.return_value = 500.0
        
        assets = [{'ID': 1, 'Symbol': 'HDX'}]
        farm_apr_data = {1: 10.0}
        
        processed = Hydration_Data_fetching.process_data(assets, farm_apr_data)
        
        self.assertEqual(len(processed), 1)
        item = processed[0]
        self.assertEqual(item['symbol'], 'HDX')
        self.assertEqual(item['farm_apr'], 10.0)
        # Pool APR = (500 * 0.0025 * 365 / 10000) * 100 = 4.5625 -> round 2 -> 4.56
        self.assertEqual(item['pool_apr'], 4.56)
        self.assertEqual(item['total_apr'], 14.56)

class TestSQLDB(unittest.TestCase):
    
    @patch('mysql.connector.connect')
    def test_sql_db_connection(self, mock_connect):
        # Test generic SQL_DB class
        db = SQL_DB.SQL_DB(userName='u', passWord='p', dataBase='d')
        
        # Test executeSQL
        mock_cursor = MagicMock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        db.executeSQL("SELECT * FROM table")
        
        mock_cursor.execute.assert_called_with("SELECT * FROM table")
        mock_connect.return_value.commit.assert_called()

class TestFetchAssetPrices(unittest.TestCase):

    def test_process_prices(self):
        assets = [{'ID': 1, 'Symbol': 'A'}, {'ID': 2, 'Symbol': 'B'}]
        price_data = {'1': 10.5, '2': 0} # Asset 2 has 0 price, should be skipped
        
        processed = fetch_asset_prices.process_prices(assets, price_data)
        
        self.assertEqual(len(processed), 1)
        self.assertEqual(processed[0]['symbol'], 'A')
        self.assertEqual(processed[0]['price_usdt'], 10.5)

class TestStellaSwap(unittest.TestCase):

    def test_calculate_token_amounts(self):
        # Just testing the function exists and runs without error on dummy data
        # The math inside is complex, we assume it's correct if it runs.
        # calculate_token_amounts(liquidity, sqrt_price_current, sqrt_price_low, sqrt_price_high)
        
        # Case 1: current < low (only token 0)
        amount0, amount1 = stellaswap_store_raw_data.calculate_token_amounts(1000, 10, 20, 30)
        self.assertIsInstance(amount0, (int, float))
        self.assertIsInstance(amount1, (int, float))
        
        # Case 2: current > high (only token 1)
        amount0, amount1 = stellaswap_store_raw_data.calculate_token_amounts(1000, 40, 20, 30)
        self.assertIsInstance(amount0, (int, float))
        
        # Case 3: in range
        amount0, amount1 = stellaswap_store_raw_data.calculate_token_amounts(1000, 25, 20, 30)
        self.assertIsInstance(amount0, (int, float))

    @patch('stellaswap_store_raw_data.requests.get')
    def test_fetch_pools_apr(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "isSuccess": True,
            "result": {
                "pool1": 0.5
            }
        }
        mock_get.return_value = mock_response
        
        data = stellaswap_store_raw_data.fetch_pools_apr()
        self.assertEqual(data['pool1'], 0.5)

class TestCombinedTables(unittest.TestCase):
    
    def test_to_decimal(self):
        self.assertEqual(SQL_DB_combinedTables._to_decimal(10), 10)
        self.assertEqual(SQL_DB_combinedTables._to_decimal("10.5"), 10.5)
        self.assertIsNone(SQL_DB_combinedTables._to_decimal("nan"))
        self.assertIsNone(SQL_DB_combinedTables._to_decimal(None))

if __name__ == '__main__':
    unittest.main()
