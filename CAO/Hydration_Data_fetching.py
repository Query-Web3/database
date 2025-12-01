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
        if result.stderr:
            print(f"Warnings/Errors from getTop35Apr.ts: {result.stderr}")
        
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

# 3. Fetch pool APR from API
def fetch_pool_apr(asset_id):
    #url = f"https://api.hydradx.io/hydradx-ui/v2/stats/fees/{asset_id}"
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/fees/{asset_id}?timeframe=1mon"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                return float(data[0].get('projected_apr_perc', 0))
            return 0
        else:
            print(f"Failed to fetch pool APR for asset {asset_id}: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error fetching pool APR for asset {asset_id}: {e}")
        return 0

# 4. Calculate total APR (sum of farm APR and pool APR)
def calculate_total_apr(farm_apr, pool_apr):
    return farm_apr + pool_apr

# 5. Fetch TVL from API
def fetch_tvl(asset_id):
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/tvl/{asset_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                return float(data[0].get('tvl_usd', 0))
            return 0
        else:
            print(f"Failed to fetch TVL for asset {asset_id}: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error fetching TVL for asset {asset_id}: {e}")
        return 0

# 6. Fetch latest volume from API
def fetch_latest_volume(asset_id):
    url = f"https://hydradx-api-app-2u5klwxkrq-ey.a.run.app/hydradx-ui/v1/stats/charts/volume/{asset_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                latest = data[-1]
                return float(latest.get('volume_usd', 0))
            return 0
        else:
            print(f"Failed to fetch volume for asset {asset_id}: {response.status_code}")
            return 0
    except Exception as e:
        print(f"Error fetching volume for asset {asset_id}: {e}")
        return 0

# Process data only for assets in farm_apr.json
def process_data(assets, farm_apr_data):
    processed_data = []
    
    # Create a lookup dictionary for symbols from assets
    symbol_lookup = {str(asset['ID']): asset['Symbol'] for asset in assets}
    
    # Only process assets present in farm_apr_data
    for asset_id in farm_apr_data.keys():
        asset_id_str = str(asset_id)  # Ensure string consistency
        symbol = symbol_lookup.get(asset_id_str, 'N/A')  # Default to 'N/A' if not found
        
        # Fetch data
        farm_apr = float(farm_apr_data.get(asset_id, 0))
        pool_apr = fetch_pool_apr(asset_id)
        total_apr = calculate_total_apr(farm_apr, pool_apr)
        tvl = fetch_tvl(asset_id)
        volume = fetch_latest_volume(asset_id)
        
        # Structure the data
        asset_data = {
            'asset_id': asset_id_str,
            'symbol': symbol,
            'farm_apr': farm_apr,
            'pool_apr': pool_apr,
            'total_apr': total_apr,
            'tvl_usd': tvl,
            'volume_usd': volume,
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
        db_port=db_port
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
                print(f"  Farm APR: {data['farm_apr']}%")
                print(f"  Pool APR: {data['pool_apr']}%")
                print(f"  Total APR: {data['total_apr']}%")
                print(f"  TVL USD: {data['tvl_usd']}")
                print(f"  Volume USD: {data['volume_usd']}")
                print("---")
            
            # Store in database
            sql_db.update_hydration_database(processed_data, batch_id)
            
            print("\nSleeping for 12 hour...")
            time.sleep(43200)
    
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
