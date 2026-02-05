import pandas as pd
import subprocess
import json
import os
import time
from dotenv import load_dotenv
from SQL_DB_hydration_price import SQL_DB_Hydration_Price
from logging_config import logger
from utils import retry, generate_batch_id, DataValidator, LivelinessProbe

# Load env vars handled inside run_pipeline or globally if script run directly
# We can leave the global load for backward compatibility if imported, but for now let's wrap it.


# Load assets from allAssets.csv (relative to this script)
def load_assets():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(script_dir, "allAssets.csv")
        df = pd.read_csv(csv_path)
        return df[df['ID'] < 30][['ID', 'Symbol']].to_dict('records')
    except FileNotFoundError:
        logger.error("allAssets.csv not found.")
        return []
    except Exception as e:
        logger.error(f"Error loading assets: {e}")
        return []

# Fetch batch prices for assets 0 to 30
@retry(max_retries=3, delay=5)
def fetch_batch_prices():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "hy/script/getBatchPrice2.ts")
    try:
        result = subprocess.run(
            ["npx", "tsx", script_path],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,  # Add 2 minute timeout
            cwd=script_dir # Ensure npx finds local modules
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
        raise # Raise to trigger retry
    except subprocess.TimeoutExpired as e:
        logger.error(f"Timeout running getBatchPrice.ts: {e}")
        raise # Raise to trigger retry
    except Exception as e:
        logger.error(f"Error fetching batch prices: {e}")
        raise # Raise to trigger retry

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
# Main execution with 30-minute interval
def run_pipeline(db_config=None, single_run=False):
    # Load environment variables
    load_dotenv()
    
    if db_config:
        db_user = db_config.get('user')
        db_password = db_config.get('password')
        db_name = db_config.get('database')
        db_port = db_config.get('port', 3306)
        db_host = db_config.get('host', '127.0.0.1')
    else:
        db_user = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")
        db_port = int(os.getenv("DB_PORT",3306))
        db_host = os.getenv("DB_HOST", "127.0.0.1")

        # Validate environment variables only if not injecting config
        required_env_vars = {"DB_USERNAME": db_user, "DB_PASSWORD": db_password, "DB_NAME": db_name}
        for var_name, var_value in required_env_vars.items():
            if not var_value:
                # If running simply as python script, this might raise. 
                # If imported, we might want to suppress or handle differently.
                pass 

    sql_db = SQL_DB_Hydration_Price(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        initializeTable=True,
        db_port=db_port,
        host=db_host,
        table_names=db_config.get('table_names') if (db_config and isinstance(db_config, dict)) else None
    )
    
    try:
        while True:
            logger.info("Fetching asset prices...")
            
            assets = load_assets()
            if not assets:
                logger.warning("No assets to process. Retrying in 30 minutes...")
                if single_run: return
                time.sleep(1800)  # 30 minutes
                continue
            
            # batch_id = int(time.time()) # Moved generation to after deduplication check
            price_data = fetch_batch_prices()
            if not price_data:
                logger.error("Failed to fetch batch prices. Retrying in 30 minutes...")
                if single_run: return
                time.sleep(1800)
                continue
            
            processed_data = process_prices(assets, price_data)

            # --- Validation ---
            if not DataValidator.validate_struct(processed_data, {'asset_id', 'symbol', 'price_usdt'}):
                logger.error("Data validation failed (structure). Skipping batch.")
                if single_run: return
                time.sleep(1800)
                continue
            if not DataValidator.validate_positive_floats(processed_data, {'price_usdt'}):
                logger.error("Data validation failed (negative prices). Skipping batch.")
                if single_run: return
                time.sleep(1800)
                continue

            # --- Deduplication ---
            current_hash = DataValidator.compute_hash(processed_data)
            last_hash = sql_db.get_last_price_hash()

            if current_hash and current_hash == last_hash:
                logger.info("Duplicate price data detected. Skipping DB update.")
            else:
                batch_id = generate_batch_id()
                sql_db.update_hydration_prices(processed_data, batch_id, data_hash=current_hash)
            
            if single_run:
                logger.info("Single run completed.")
                break

            LivelinessProbe.record_heartbeat("prices")
            logger.info("Sleeping for 1 hour...")
            time.sleep(3600)  # 1 hour sleep
    
    except Exception as e:
        logger.exception(f"Error occurred in fetch_asset_prices main loop: {e}")

if __name__ == "__main__":
    run_pipeline()
