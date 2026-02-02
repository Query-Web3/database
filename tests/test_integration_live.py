import pytest
import pandas as pd
import time
from CAO.Bifrost_Data_fetching import run_pipeline as run_bifrost_pipeline
from CAO.fetch_asset_prices import run_pipeline as run_prices_pipeline
from CAO.SQL_DB import SQL_DB
from CAO.SQL_DB_hydration_price import SQL_DB_Hydration_Price

@pytest.mark.integration
def test_bifrost_pipeline_live(setup_test_db):
    """
    Test the full Bifrost data pipeline:
    1. Fetch live data from Bifrost API.
    2. Insert into TEST tables in PROD database.
    3. Verify data exists in these TEST tables.
    """
    db_config = setup_test_db
    print(f"Running Bifrost pipeline against DB: {db_config['database']} (Test Tables)")
    
    # Run pipeline in single-run mode
    run_bifrost_pipeline(db_config=db_config, single_run=True)
    
    # Verify DB content using config (so it queries TEST tables)
    db = SQL_DB(db_config=db_config)
    
    # Check Bifrost_site_table
    # Note: get_last_bifrost_hash uses correct table internally now
    
    # We can also execute raw SQL using dynamic table names
    table_site = db_config['table_names']['Bifrost_site_table']
    df_site = db.executeSQL(f"SELECT * FROM {table_site} ORDER BY created_at DESC LIMIT 5")
    assert df_site is not None, f"{table_site} should have data"
    assert len(df_site) > 0, f"{table_site} should not be empty"
    
    # Check Bifrost_staking_table
    table_staking = db_config['table_names']['Bifrost_staking_table']
    df_staking = db.executeSQL(f"SELECT * FROM {table_staking} ORDER BY created_at DESC LIMIT 5")
    assert df_staking is not None, f"{table_staking} should have data"
    assert len(df_staking) > 0, f"{table_staking} should not be empty"

    # Check Hash
    last_hash = db.get_last_bifrost_hash()
    assert last_hash is not None, "Should have generated a data hash"


@pytest.mark.integration
def test_prices_pipeline_live(setup_test_db):
    """
    Test the full Asset Price pipeline:
    1. Fetch live prices via TS script (npx tsx).
    2. Insert into TEST tables in PROD database.
    3. Verify data exists in TEST tables.
    """
    db_config = setup_test_db
    print(f"Running Prices pipeline against DB: {db_config['database']} (Test Tables)")

    # Run pipeline in single-run mode
    run_prices_pipeline(db_config=db_config, single_run=True)
    
    # Verify DB content
    # Note: SQL_DB_Hydration_Price supports table_names injection in init now
    db = SQL_DB_Hydration_Price(
        userName=db_config['user'],
        passWord=db_config['password'],
        host=db_config['host'],
        dataBase=db_config['database'],
        db_port=db_config['port'],
        table_names=db_config['table_names']
    )
    
    # Check Hydration_price
    table_price = db_config['table_names']['Hydration_price']
    res = db.executeSQL(f"SELECT * FROM {table_price} ORDER BY created_at DESC LIMIT 5")
    assert res is not None, f"{table_price} table should have data"
    assert len(res) > 0, f"{table_price} table should not be empty"

    # Verify meaningful data (prices > 0)
    # Schema: id, batch_id, asset_id, symbol, price_usdt, created_at
    # Indices: 0, 1,        2,        3,      4,          5
    prices = [row[4] for row in res] 
    
    assert any(p > 0 for p in prices), "At least some prices should be positive"
