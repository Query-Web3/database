This documentation outlines the components and flow of the system used to collect DeFi asset metrics from the Hydration SDK and store them into a MySQL-compatible SQL database for further analysis and tracking.

The system includes the following core scripts:

Hydration_Data_fetching.py: Responsible for retrieving APR and TVL data from the Hydration SDK.

SQL_DB_hydration.py: Handles storage of the fetched data into the database.


1. hydration_data Table (SQL Schema)
This table stores APR (Annual Percentage Rate), TVL (Total Value Locked), and trading volume data from the Hydration protocol for each asset. It’s structured as follows:
CREATE TABLE IF NOT EXISTS hydration_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id INT NOT NULL,
    asset_id VARCHAR(50),
    symbol VARCHAR(50),
    farm_apr DOUBLE,
    pool_apr DOUBLE,
    total_apr DOUBLE,
    tvl_usd DOUBLE,
    volume_usd DOUBLE,
    timestamp VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


Field Descriptions:

id: Unique auto-incremented identifier.

batch_id: An identifier that groups all data collected in a single fetch cycle.

asset_id: Unique identifier of the asset.

symbol: Ticker symbol (e.g., WETH, DAI).

farm_apr: Annual yield from farming rewards.

pool_apr: Yield from providing liquidity to pools.

total_apr: Combined APR (farming + pool).

tvl_usd: Total Value Locked in USD.

volume_usd: Trading volume over a given timeframe.

timestamp: Time when the data was fetched (could be epoch or readable string).

created_at: Auto-generated timestamp indicating when the record was inserted.


2. SQL_DB_hydration.py
This script manages:

SQL database connection setup.

Table creation if not already existing.

Insertion of hydration data into the hydration_data table.

3. Hydration_Data_fetching.py
This is the main script executed on a scheduled basis (e.g., daily or hourly). It performs the following:

Connects to the Hydration SDK.

Fetches APR, TVL, and volume data for all assets.

Assigns a new batch_id to group this round of data.

Passes the formatted data to SQL_DB_hydration.py for database insertion.



