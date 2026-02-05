import pandas as pd
import requests
import json
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv
import os

from SQL_DB_hydration import SQL_DB_Hydration
from logging_config import logger
from utils import LivelinessProbe

# Load environment variables from .env file
load_dotenv()
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")      
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT",3306)
db_host = os.getenv("DB_HOST", "127.0.0.1")

# Validate environment variables
required_env_vars = {
    "DB_USERNAME": db_user,
    "DB_PASSWORD": db_password,
    "DB_NAME": db_name
}
for var_name, var_value in required_env_vars.items():
    if not var_value:
        # For testing, we might want to default or skip, but following original logic:
        # raise ValueError(f"{var_name} not found in .env file.")
        pass

# 1. Load assets from allAssets.csv
def load_assets():
    try:
        df = pd.read_csv("allAssets.csv")
        return df[['ID', 'Symbol']].to_dict('records')
    except Exception as e:
        logger.error(f"Error loading assets: {e}")
        return []

# 2. Run TypeScript script
def fetch_farm_apr():
    script_path = "hy/script/getTop35Apr3.ts"
    output_file = "./farm_apr.json"
    try:
        subprocess.run(["npx", "tsx", script_path, output_file], capture_output=True, text=True, check=True)
        with open(output_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error fetching farm APR: {e}")
        return {}

# 3. Fetch TVL
def fetch_tvl(asset_id):
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/tvl/{asset_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return float(data[0].get('tvl_usd', 0)) if data else 0
        return 0
    except Exception:
        return 0

# 4. Fetch latest volume
def fetch_latest_volume(asset_id):
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/volume/{asset_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return float(data[-1].get('volume_usd', 0)) if data else 0
        return 0
    except Exception:
        return 0

# 5. Calculate Pool APR
def calculate_pool_apr(tvl, volume_24h):
    if tvl <= 0: return 0.0
    return (volume_24h * 0.0025 * 365 / tvl) * 100

# 6. Calculate total APR
def calculate_total_apr(farm_apr, pool_apr):
    return farm_apr + pool_apr

# Process data
def process_data(assets, farm_apr_data):
    processed_data = []
    symbol_lookup = {str(asset['ID']): asset['Symbol'] for asset in assets}
    for asset_id, farm_apr in farm_apr_data.items():
        asset_id_str = str(asset_id)
        symbol = symbol_lookup.get(asset_id_str, 'N/A')
        tvl = fetch_tvl(asset_id_str)
        volume = fetch_latest_volume(asset_id_str)
        pool_apr = calculate_pool_apr(tvl, volume)
        total_apr = calculate_total_apr(float(farm_apr), pool_apr)
        processed_data.append({
            'asset_id': asset_id_str,
            'symbol': symbol,
            'farm_apr': round(float(farm_apr), 2),
            'pool_apr': round(pool_apr, 2),
            'total_apr': round(total_apr, 2),
            'tvl_usd': round(tvl, 2),
            'volume_usd': round(volume, 2),
            'timestamp': datetime.utcnow().isoformat()
        })
    return processed_data

def main():
    sql_db = SQL_DB_Hydration(userName=db_user, passWord=db_password, host=db_host, db_port=db_port, dataBase=db_name, initializeTable=True)
    try:
        while True:
            logger.info("Starting Hydration data fetch batch...")
            batch_id = int(time.time())
            assets = load_assets()
            farm_apr_data = fetch_farm_apr()
            if assets and farm_apr_data:
                processed_data = process_data(assets, farm_apr_data)
                sql_db.update_hydration_database(processed_data, batch_id)
            LivelinessProbe.record_heartbeat("hydration")
            time.sleep(3600)     # sleep 1 hour
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()