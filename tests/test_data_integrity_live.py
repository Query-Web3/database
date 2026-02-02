import pytest
import os
import sys
import pandas as pd
from pathlib import Path
from unittest.mock import patch

# Add CAO and project root to path
project_root = Path(__file__).resolve().parent.parent
cao_dir = project_root / 'CAO'
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(cao_dir))

from SQL_DB import SQL_DB
from Bifrost_Data_fetching import run_pipeline, sanitize_df
from utils import generate_batch_id, DataValidator

@pytest.fixture(scope="session")
def db_config():
    """Provides test database configuration from .env."""
    from dotenv import load_dotenv
    load_dotenv(cao_dir / '.env', override=True)
    
    return {
        'user': os.environ.get('DB_USERNAME'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': int(os.environ.get('DB_PORT', 3306)),
        'database': os.environ.get('DB_NAME', 'QUERYWEB3'),
        'table_names': {
            'Bifrost_site_table': 'TEST_Bifrost_site',
            'Bifrost_staking_table': 'TEST_Bifrost_staking',
            'Bifrost_batchID_table': 'TEST_Bifrost_batchID'
        }
    }

@pytest.fixture(autouse=True)
def setup_test_tables(db_config):
    """Initializes and cleans up test tables."""
    db = SQL_DB(db_config=db_config, initializeTable=True)
    
    # Clean up before test
    db.executeSQL(f"DELETE FROM {db.tables['Bifrost_site_table']}")
    db.executeSQL(f"DELETE FROM {db.tables['Bifrost_staking_table']}")
    db.executeSQL(f"DELETE FROM {db.tables['Bifrost_batchID_table']}")
    
    yield db
    
    # Optional: cleanup after test (commented out to allow inspection on failure)
    # db.executeSQL(f"DROP TABLE IF EXISTS {db.tables['Bifrost_site_table']}")
    # db.executeSQL(f"DROP TABLE IF EXISTS {db.tables['Bifrost_staking_table']}")
    # db.executeSQL(f"DROP TABLE IF EXISTS {db.tables['Bifrost_batchID_table']}")

@pytest.mark.integration
class TestDataIntegrity:
    
    def test_bifrost_persistence_and_deduplication(self, setup_test_tables, db_config):
        """Verifies that data is correctly written and duplicates are skipped."""
        db = setup_test_tables
        
        # 1. Mock API data
        mock_df1 = pd.DataFrame([{"Asset": "DOT", "Value": 100.5}])
        mock_df2 = pd.DataFrame([{"symbol": "vDOT", "apr": 0.15}])
        
        # 2. Run pipeline (Single Run)
        with patch('Bifrost_Data_fetching.fetch_data', return_value=mock_df1):
            with patch('Bifrost_Data_fetching.fetch_data2', return_value=mock_df2):
                run_pipeline(db_config=db_config, single_run=True)
        
        # 3. Assert data in DB
        res_site = db.executeSQL(f"SELECT COUNT(*) FROM {db.tables['Bifrost_site_table']}")
        assert res_site[0][0] == 1, "Should have inserted one row into site table"
        
        res_staking = db.executeSQL(f"SELECT COUNT(*) FROM {db.tables['Bifrost_staking_table']}")
        assert res_staking[0][0] == 1, "Should have inserted one row into staking table"
        
        res_batch = db.executeSQL(f"SELECT batch_id, data_hash FROM {db.tables['Bifrost_batchID_table']} LIMIT 1")
        assert len(res_batch) == 1, "Should have tracked batch ID"
        first_batch_id = res_batch[0][0]
        first_hash = res_batch[0][1]
        assert first_hash is not None, "Hash should be computed and stored"
        
        # 4. Run again with SAME data (Deduplication Check)
        with patch('Bifrost_Data_fetching.fetch_data', return_value=mock_df1):
            with patch('Bifrost_Data_fetching.fetch_data2', return_value=mock_df2):
                run_pipeline(db_config=db_config, single_run=True)
        
        # Count should still be 1
        res_site_2 = db.executeSQL(f"SELECT COUNT(*) FROM {db.tables['Bifrost_site_table']}")
        assert res_site_2[0][0] == 1, "Deduplication failed: Duplicate row inserted into site table"
        
        res_batch_count = db.executeSQL(f"SELECT COUNT(*) FROM {db.tables['Bifrost_batchID_table']}")
        assert res_batch_count[0][0] == 1, "Deduplication failed: Secondary batch entry created"
        
        # 5. Run with NEW data
        mock_df1_new = pd.DataFrame([{"Asset": "DOT", "Value": 101.0}]) # Different value
        with patch('Bifrost_Data_fetching.fetch_data', return_value=mock_df1_new):
            with patch('Bifrost_Data_fetching.fetch_data2', return_value=mock_df2):
                run_pipeline(db_config=db_config, single_run=True)
                
        # Count should be 2
        res_site_3 = db.executeSQL(f"SELECT COUNT(*) FROM {db.tables['Bifrost_site_table']}")
        assert res_site_3[0][0] == 2, "Should have inserted new row for changed data"
        
        res_batch_count_2 = db.executeSQL(f"SELECT COUNT(*) FROM {db.tables['Bifrost_batchID_table']}")
        assert res_batch_count_2[0][0] == 2, "Should have tracked second batch ID"

    def test_batch_id_monotonicity(self, setup_test_tables, db_config):
        """Verifies that batch IDs are unique and generally increasing (timestamp-based)."""
        db = setup_test_tables
        
        # Generate two IDs with a small sleep
        id1 = generate_batch_id()
        import time
        time.sleep(1.1)
        id2 = generate_batch_id()
        
        assert id2 > id1, "Batch IDs should be strictly increasing integers"

    def test_data_sanitization_integrity(self, setup_test_tables, db_config):
        """Verifies that handled NaN/Inf are written as NULL to MySQL."""
        db = setup_test_tables
        
        import numpy as np
        dirty_df = pd.DataFrame([{
            "Asset": "DIRTY",
            "Value": np.nan,
            "tvl": np.inf
        }])
        
        # Sanitize
        clean_df = sanitize_df(dirty_df)
        
        # Insert directly to test SQL_DB's internal cleanup
        db.update_bifrost_database(clean_df, None, 999999, data_hash="test_san")
        
        # Verify NULLs in DB
        res = db.executeSQL(f"SELECT Value, tvl FROM {db.tables['Bifrost_site_table']} WHERE batch_id=999999")
        assert res[0][0] is None, "NaN should be stored as NULL"
        assert res[0][1] is None, "Inf should be stored as NULL"
