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
from dotenv import load_dotenv

# Add project root and CAO directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cao_dir = os.path.join(project_root, 'CAO')
sys.path.insert(0, project_root)
sys.path.insert(0, cao_dir)

# Construct path to .env
env_path = os.path.join(cao_dir, '.env')

print(f"DEBUG: Project Root: {project_root}")
print(f"DEBUG: CAO Dir: {cao_dir}")
print(f"DEBUG: Env Path: {env_path}")

if os.path.exists(env_path):
    print(f"DEBUG: .env file found at {env_path}")
    load_dotenv(env_path)
else:
    print(f"WARNING: .env file NOT found at {env_path}")

# Check loaded vars
db_user_env = os.environ.get('DB_USERNAME')
print(f"DEBUG: DB_USERNAME from environment: {db_user_env}")

# Setup environment variables for imports - ONLY if not present
if not os.environ.get('DB_USERNAME'):
    print("WARNING: DB_USERNAME not set in environment. Tests will likely fail.")
    # We deliberately do NOT set 'test_user' here anymore to avoid confusing "Access denied" errors for wrong user
    # If the user really wants defaults, they should set them in .env or system env
    
os.environ.setdefault('DB_PORT', '3306')
os.environ.setdefault('DB_HOST', '127.0.0.1')


def pytest_configure(config):
    """Add integration marker."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (slow, live api)"
    )

@pytest.fixture(scope="session")
def test_db_config():
    """Configuration for the test database (integration)."""
    # Use real credentials but point to TEST tables
    # DEFAULT to 'root' and empty password if not set, which is standard for local mysql sometimes
    # But better to fail if not set than try 'test_user'
    
    # CRITICAL: Clean up any pollution from legacy tests that might have hardcoded 'test_user'
    for key in ['DB_USERNAME', 'DB_PASSWORD', 'DB_NAME']:
        val = os.environ.get(key)
        if val in ['test_user', 'test_pass', 'test_db']:
            print(f"DEBUG: Clearing polluted env var {key}={val}")
            del os.environ[key]

    # CRITICAL: Force reload .env to override any pollution from legacy tests
    env_path = os.path.join(cao_dir, '.env')
    if os.path.exists(env_path):
        from dotenv import dotenv_values
        env_vars = dotenv_values(env_path)
        for k, v in env_vars.items():
            if v: # Only set if it has a value
                os.environ[k] = v
        print(f"DEBUG: Force-reloaded .env from {env_path}")
    
    user = os.environ.get('DB_USERNAME')
    password = os.environ.get('DB_PASSWORD', '')
    host = os.environ.get('DB_HOST', '127.0.0.1')
    port = int(os.environ.get('DB_PORT', 3306))
    database = os.environ.get('DB_NAME', 'bot_database')
    
    if not user:
        print("CRITICAL: DB_USERNAME is not set. Database connection will likely fail.")

    print(f"DEBUG: Final DB config - user: {user}, database: {database}, host: {host}")

    return {
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database': database,
        'table_names': {
             "Bifrost_site_table": "TEST_Bifrost_site_table",
             "Bifrost_staking_table": "TEST_Bifrost_staking_table",
             "Bifrost_batchID_table": "TEST_Bifrost_batchID_table",
             "Hydration_price": "TEST_Hydration_price",
             "Hydration_price_batches": "TEST_Hydration_price_batches"
        }
    }

@pytest.fixture(scope="session")
def setup_test_db(test_db_config):
    """
    Initializes TEST tables in the existing DB.
    Yields the database configuration.
    Drops the TEST tables after the session.
    """
    import mysql.connector
    from CAO.SQL_DB import SQL_DB
    from CAO.SQL_DB_hydration_price import SQL_DB_Hydration_Price
    
    print(f"Initializing test tables in DB: {test_db_config.get('database')}")
    
    try:
        # 1. Initialize tables (Create if not exist)
        # Using SQL_DB class
        db = SQL_DB(db_config=test_db_config, initializeTable=True)
        
        # Using SQL_DB_Hydration_Price class
        db_price = SQL_DB_Hydration_Price(
            userName=test_db_config['user'],
            passWord=test_db_config['password'],
            host=test_db_config['host'],
            dataBase=test_db_config['database'],
            db_port=test_db_config['port'],
            initializeTable=True,
            table_names=test_db_config['table_names']
        )
        
        print("Test tables initialized.")
        yield test_db_config
        
        # Teardown
        print("Dropping test tables...")
        conn = mysql.connector.connect(
            user=test_db_config['user'],
            password=test_db_config['password'],
            host=test_db_config['host'],
            database=test_db_config['database'],
            port=test_db_config['port']
        )
        cursor = conn.cursor()
        for key, table_name in test_db_config['table_names'].items():
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        cursor.close()
        conn.close()
        print("Test tables dropped.")
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        pytest.fail(f"Failed to setup test database: {e}")


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
