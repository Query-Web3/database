This document describes the system components and workflow for fetching real-time asset prices and storing them into a structured SQL database. The system consists of two main Python scripts:

fetch_asset_prices.py: The main script that fetches asset prices from an external API or data source.

SQL_DB_hydration_price.py: A utility script responsible for storing the fetched prices into a MySQL (or compatible) database.

Hydration_price Table (SQL)
This table stores the historical price records of various digital assets in USD (USDT-equivalent). It is created using: 
CREATE TABLE IF NOT EXISTS Hydration_price (
    id INT AUTO_INCREMENT PRIMARY KEY,
    batch_id INT NOT NULL,
    asset_id VARCHAR(50),
    symbol VARCHAR(50),
    price_usdt DOUBLE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


id: Auto-incrementing unique identifier.

batch_id: A group ID for tracking price fetches done together.

asset_id: Internal or external identifier for the asset.

symbol: Ticker symbol (e.g., BTC, ETH).

price_usdt: Price in USDT.

created_at: Timestamp for when the data was recorded.




** SQL_DB_hydration_price.py
This script is responsible for:

Connecting to the SQL database.

Creating the Hydration_price table (if not exists).

Inserting new price records.


** fetch_asset_prices.py
This is the main execution script. It does the following:

Calls Hydration external API(s) to fetch asset prices.

Generates a new batch_id for each fetch cycle.

Formats the data into a structure compatible with the Hydration_price table.

Calls insert_prices() from SQL_DB_hydration_price.py to store the results.
