"""
Shared pytest fixtures for database project tests.

This module provides common fixtures for mocking database connections,
API responses, and generating test data.
"""

import pytest
from unittest.mock import MagicMock, Mock
import pandas as pd
import numpy as np
import os
import sys

# Add project root and CAO directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, project_root)
sys.path.insert(0, cao_dir)

# Setup environment variables for imports
os.environ.setdefault('DB_USERNAME', 'test_user')
os.environ.setdefault('DB_PASSWORD', 'test_pass')
os.environ.setdefault('DB_NAME', 'test_db')
os.environ.setdefault('DB_PORT', '3306')
os.environ.setdefault('DB_HOST', '127.0.0.1')
os.environ.setdefault('API_KEY', 'test_api_key')


@pytest.fixture
def mock_db_connection():
    """Mock MySQL connection object."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Setup cursor behavior
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_cursor.description = None
    
    # Setup connection behavior
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None
    mock_conn.close.return_value = None
    
    return mock_conn, mock_cursor


@pytest.fixture
def mock_mysql_connector(mock_db_connection):
    """Mock mysql.connector.connect to return mock connection."""
    mock_conn, mock_cursor = mock_db_connection
    
    def _connect(*args, **kwargs):
        return mock_conn
    
    return _connect


@pytest.fixture
def sample_bifrost_api_response():
    """Sample Bifrost API response data."""
    return {
        "DOT": {
            "price": 5.0,
            "tvl": 1000000,
            "apy": 12.5
        },
        "KSM": {
            "price": 25.0,
            "tvl": 500000,
            "apy": 15.0
        }
    }


@pytest.fixture
def sample_bifrost_staking_response():
    """Sample Bifrost staking API response."""
    return {
        "supportedAssets": [
            {
                "contractAddress": "0x123",
                "symbol": "vDOT",
                "slug": "vdot",
                "baseSlug": "dot",
                "unstakingTime": 28,
                "users": 1000,
                "apr": 15.5,
                "fee": 10,
                "price": 5.25,
                "exchangeRatio": 1.05,
                "supply": 100000.0
            }
        ]
    }


@pytest.fixture
def sample_hydration_tvl_response():
    """Sample Hydration TVL API response."""
    return [
        {
            "tvl_usd": 10000.0,
            "timestamp": "2024-01-01T00:00:00Z"
        }
    ]


@pytest.fixture
def sample_hydration_volume_response():
    """Sample Hydration volume API response."""
    return [
        {
            "volume_usd": 500.0,
            "timestamp": "2024-01-01T00:00:00Z"
        }
    ]


@pytest.fixture
def sample_stellaswap_pools_apr():
    """Sample Stellaswap pools APR response."""
    return {
        "isSuccess": True,
        "result": {
            "pool1": 0.5,
            "pool2": 0.75
        }
    }


@pytest.fixture
def sample_stellaswap_farming_apr():
    """Sample Stellaswap farming APR response."""
    return {
        "isSuccess": True,
        "result": {
            "pool1": 10.0,
            "pool2": 12.5
        }
    }


@pytest.fixture
def sample_dataframe():
    """Sample pandas DataFrame for testing."""
    return pd.DataFrame({
        'Asset': ['DOT', 'KSM'],
        'price': [5.0, 25.0],
        'tvl': [1000000, 500000],
        'apy': [12.5, 15.0]
    })


@pytest.fixture
def sample_dataframe_with_nan():
    """Sample DataFrame with NaN, Inf values for sanitization testing."""
    return pd.DataFrame({
        'A': ['NaN', 'Inf', 1.0],
        'B': [np.inf, -np.inf, 2.0],
        'C': ['1.5', 'nan', '3.0'],
        'D': [None, 'test', 5.0]
    })


@pytest.fixture
def sample_hydration_assets():
    """Sample assets list for Hydration testing."""
    return [
        {'ID': 1, 'Symbol': 'HDX'},
        {'ID': 2, 'Symbol': 'DOT'},
        {'ID': 5, 'Symbol': 'USDT'}
    ]


@pytest.fixture
def sample_farm_apr_data():
    """Sample farm APR data."""
    return {
        1: 10.5,
        2: 8.25,
        5: 5.0
    }


@pytest.fixture
def sample_processed_hydration_data():
    """Sample processed Hydration data."""
    return [
        {
            'asset_id': '1',
            'symbol': 'HDX',
            'farm_apr': 10.5,
            'pool_apr': 4.56,
            'total_apr': 15.06,
            'tvl_usd': 10000.0,
            'volume_usd': 500.0,
            'timestamp': '2024-01-01T00:00:00'
        }
    ]


@pytest.fixture
def sample_batch_id():
    """Sample batch ID for testing."""
    return 1705881600  # Unix timestamp


@pytest.fixture
def cleanup_env():
    """Cleanup fixture to reset environment after tests."""
    yield
    # Reset environment variables if needed
    pass
