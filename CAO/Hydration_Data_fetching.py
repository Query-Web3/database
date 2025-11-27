import pandas as pd
import requests
import json
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv
import os

from SQL_DB_hydration import SQL_DB_Hydration

# Load environment variables from .env file
load_dotenv()
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")      
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT",3306)

# Validate environment variables
required_env_vars = {
    "DB_USERNAME": db_user,
    "DB_PASSWORD": db_password,
    "DB_NAME": db_name
}
for var_name, var_value in required_env_vars.items():
    if not var_value:
        raise ValueError(f"{var_name} not found in .env file. Please set it and try again.")

# 1. Load assets from allAssets.csv
def load_assets():
    try:
        df = pd.read_csv("allAssets.csv")
        return df[['ID', 'Symbol']].to_dict('records')  # List of dicts with 'ID' and 'Symbol'
    except FileNotFoundError:
        print("Error: allAssets.csv not found.")
        return []
    except Exception as e:
        print(f"Error loading assets: {e}")
        return []

# 2. Run TypeScript script to update farm_apr.json and load the result
def fetch_farm_apr():
    script_path = "sdk/packages/sdk/test/script/examples/getTop35Apr3.ts"
    output_file = "./farm_apr.json"
    
    try:
        print(f"Running {script_path} to update {output_file}...")
        result = subprocess.run(
            ["npx", "tsx", script_path, output_file],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"getTop35Apr.ts output: {result.stdout}")
        
        with open(output_file, 'r') as f:
            farm_apr_data = json.load(f)
        print(f"Loaded farm APR data from {output_file}")
        return farm_apr_data  # {assetID: farm_apr}
    except subprocess.CalledProcessError as e:
        print(f"Error running getTop35Apr.ts: {e.stderr}")
        return {}
    except FileNotFoundError:
        print(f"Error: {output_file} not found after running script.")
        return {}
    except Exception as e:
        print(f"Error fetching farm APR: {e}")
        return {}

# 3. Fetch TVL from API
def fetch_tvl(asset_id):
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/tvl/{asset_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # The API usually returns history, index 0 is typically the latest or oldest depending on sort
                # For this specific API, usually [0] is latest, but let's be safe.
                return float(data[0].get('tvl_usd', 0))
            return 0
        else:
            print(f"Failed to fetch TVL for asset {asset_id}: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error fetching TVL for asset {asset_id}: {e}")
        return 0

# 4. Fetch latest volume from API
def fetch_latest_volume(asset_id):
    # Using the charts endpoint to get the last bucket of volume
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/volume/{asset_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # Get the last entry in the chart (latest day)
                latest = data[-1]
                return float(latest.get('volume_usd', 0))
            return 0
        else:
            print(f"Failed to fetch volume for asset {asset_id}: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error fetching volume for asset {asset_id}: {e}")
        return 0

# 5. Calculate Pool APR manually
def calculate_pool_apr(tvl, volume_24h):
    """
    Calculates APR based on 24h Volume and TVL.
    Formula: (Volume * Fee * 365) / TVL
    Standard Hydration Fee = 0.25% (0.0025)
    """
    if tvl <= 0:
        return 0.0
        
    FEE_RATE = 0.0025 # 0.25%
    
    daily_fees = volume_24h * FEE_RATE
    yearly_fees = daily_fees * 365
    apr = (yearly_fees / tvl) * 100 # Convert to percentage
    
    return apr

# 6. Calculate total APR (sum of farm APR and pool APR)
def calculate_total_apr(farm_apr, pool_apr):
    return farm_apr + pool_apr

# Process data only for assets in farm_apr.json
def process_data(assets, farm_apr_data):
    processed_data = []
    
    # Create a lookup dictionary for symbols from assets
    symbol_lookup = {str(asset['ID']): asset['Symbol'] for asset in assets}
    
    # Only process assets present in farm_apr_data
    for asset_id in farm_apr_data.keys():
        asset_id_str = str(asset_id)
        symbol = symbol_lookup.get(asset_id_str, 'N/A')
        
        # 1. Fetch TVL & Volume FIRST (Needed for calc)
        tvl = fetch_tvl(asset_id)
        volume = fetch_latest_volume(asset_id)
        
        # 2. Get Farm APR (from local JSON)
        farm_apr = float(farm_apr_data.get(asset_id, 0))
        
        # 3. Calculate Pool APR manually
        pool_apr = calculate_pool_apr(tvl, volume)
        
        # 4. Total
        total_apr = calculate_total_apr(farm_apr, pool_apr)
        
        # Structure the data
        asset_data = {
            'asset_id': asset_id_str,
            'symbol': symbol,
            'farm_apr': round(farm_apr, 2),
            'pool_apr': round(pool_apr, 2),
            'total_apr': round(total_apr, 2),
            'tvl_usd': round(tvl, 2),
            'volume_usd': round(volume, 2),
            'timestamp': datetime.utcnow().isoformat()
        }
        processed_data.append(asset_data)
    
    return processed_data

# Main execution
def main():
    sql_db = SQL_DB_Hydration(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        initializeTable=True,
        db_port=db_port,
        ssl_disabled=True
    )
    
    try:
        while True:
            print("\nFetching Hydration data...")
            batch_id = int(time.time())
            
            # Load assets and fetch data
            assets = load_assets()
            if not assets:
                print("No assets to process. Retrying in 1 hour...")
                time.sleep(3600)
                continue
            
            farm_apr_data = fetch_farm_apr()
            if not farm_apr_data:
                print("Failed to fetch farm APR. Retrying in 1 hour...")
                time.sleep(3600)
                continue
            
            processed_data = process_data(assets, farm_apr_data)
            
            # Print for verification
            for data in processed_data:
                print(f"Asset {data['asset_id']} ({data['symbol']}):")
                print(f"  Farm APR:  {data['farm_apr']}%")
                print(f"  Pool APR:  {data['pool_apr']}%")
                print(f"  Total APR: {data['total_apr']}%")
                print(f"  TVL USD:   ${data['tvl_usd']:,.2f}")
                print(f"  Vol USD:   ${data['volume_usd']:,.2f}")
                print("---")
            
            # Store in database
            sql_db.update_hydration_database(processed_data, batch_id)
            
            print("\nSleeping for 12 hours...")
            time.sleep(43200)
    
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()