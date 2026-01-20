import pandas as pd
import subprocess
import json
import os
import time
from dotenv import load_dotenv
from SQL_DB_hydration_price import SQL_DB_Hydration_Price
from logging_config import logger

# Load environment variables
load_dotenv()
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT",3306)
db_host = os.getenv("DB_HOST", "127.0.0.1")

# Validate environment variables
required_env_vars = {"DB_USERNAME": db_user, "DB_PASSWORD": db_password, "DB_NAME": db_name}
for var_name, var_value in required_env_vars.items():
    if not var_value:
        raise ValueError(f"{var_name} not found in .env file.")

# Load assets from allAssets.csv and filter for ID < 30
def load_assets():
    try:
        df = pd.read_csv("allAssets.csv")
        return df[df['ID'] < 30][['ID', 'Symbol']].to_dict('records')
    except FileNotFoundError:
        logger.error("allAssets.csv not found.")
        return []
    except Exception as e:
        logger.error(f"Error loading assets: {e}")
        return []

# Fetch batch prices for assets 0 to 30
def fetch_batch_prices():
    script_path = "hy/script/getBatchPrice2.ts"
    try:
        result = subprocess.run(
            ["tsx", script_path],
            capture_output=True,
            text=True,
            check=True
        )
        # check before we process the output 
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if not stdout:
            logger.error("getBatchPrice.ts produced no stdout.")
            return {}
        else:
            logger.debug(f"Raw script output: {stdout}")
        
        data = json.loads(stdout)
        logger.debug("Successfully loaded price JSON.")
        if 'error' in data:
            logger.error(f"Error fetching batch prices: {data['error']}")
            return {}
        # Convert to dict {asset_id: price}
        return {item['assetId']: item['price'] for item in data}
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running getBatchPrice.ts: {e.stderr}")
        return {}
    except Exception as e:
        logger.error(f"Error fetching batch prices: {e}")
        return {}

# Process assets and match prices
def process_prices(assets, price_data):
    processed_data = []
    
    for asset in assets:
        asset_id = str(asset['ID'])  # Ensure string for consistency
        symbol = asset['Symbol']
        price_usdt = price_data.get(asset_id, 0)  # Default to 0 if not found
        if price_usdt == 0:
            continue        # skip those  
        asset_data = {
            'asset_id': asset_id,
            'symbol': symbol,
            'price_usdt': price_usdt
        }
        processed_data.append(asset_data)
        
        logger.info(f"Asset {asset_id} ({symbol}): Price = {price_usdt} USDT")
    
    return processed_data

# Main execution with 30-minute interval
def main():
    sql_db = SQL_DB_Hydration_Price(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        initializeTable=True,
        db_port=db_port,
        host=db_host
    )
    
    try:
        while True:
            logger.info("Fetching asset prices...")
            
            assets = load_assets()
            if not assets:
                logger.warning("No assets to process. Retrying in 30 minutes...")
                time.sleep(1800)  # 30 minutes
                continue
            
            batch_id = int(time.time())
            price_data = fetch_batch_prices()
            if not price_data:
                logger.error("Failed to fetch batch prices. Retrying in 30 minutes...")
                time.sleep(1800)
                continue
            
            processed_data = process_prices(assets, price_data)
            sql_db.update_hydration_prices(processed_data, batch_id)
            
            logger.info("Sleeping for 10 minutes...")
            time.sleep(600)  # 10 minutes = 600 seconds
    
    except Exception as e:
        logger.exception(f"Error occurred in fetch_asset_prices main loop: {e}")

if __name__ == "__main__":
    main()
