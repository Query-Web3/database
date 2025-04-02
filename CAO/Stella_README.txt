Overview
This system is designed to fetch detailed liquidity pool data from the StellaSwap SDK and store it in a structured MySQL-compatible database for analytics, research, or DeFi dashboard integration.

🔧 Components
stellaswap_store_raw_data.py: Main script responsible for fetching and processing StellaSwap pool data.

SQL_DB_stella.py: Handles all SQL-related operations, including table creation and inserting pool data.

pool_data
This table stores comprehensive metrics about StellaSwap liquidity pools.
CREATE TABLE IF NOT EXISTS pool_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id INT NOT NULL,
    pool_id VARCHAR(255),
    token0_id VARCHAR(255),
    token0_symbol VARCHAR(50),
    token0_name VARCHAR(255),
    token0_decimals INT,
    token1_id VARCHAR(255),
    token1_symbol VARCHAR(50),
    token1_name VARCHAR(255),
    token1_decimals INT,
    liquidity DOUBLE,
    sqrt_price DOUBLE,
    tick INT,
    volume_usd_current DOUBLE,
    volume_usd_24h_ago DOUBLE,
    volume_usd_24h DOUBLE,
    tx_count INT,
    fees_usd_current DOUBLE,
    fees_usd_24h_ago DOUBLE,
    fees_usd_24h DOUBLE,
    amount_token0 DOUBLE,
    amount_token1 DOUBLE,
    pools_apr DOUBLE,
    farming_apr DOUBLE,
    final_apr DOUBLE,
    token_rewards TEXT,
    timestamp VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


Field Descriptions
Basic Info: pool_id, token identifiers and symbols.

Metrics: Liquidity, pricing, volume, transaction count.

Rewards/APR: Pool APR, farming APR, and combined final APR.

Historical: Volume/Fees comparisons over 24h period.

Token rewards: JSON/text representation of distributed token info.

Metadata: Timestamp and creation info.



SQL_DB_stella.py
This utility script manages:

Connection to MySQL server.

Table creation (pool_data).

Function to insert or bulk insert pool data entries into the table.




stellaswap_store_raw_data.py
This is the main data pipeline that:

Connects to StellaSwap SDK or API.

Fetches the latest pool metrics.

Assigns a batch_id for the fetch job.

Prepares data in the format required by SQL_DB_stella.py.

Invokes the insert function to store the data in SQL.


