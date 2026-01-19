import requests
import os
import math
import pandas as pd
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from SQL_DB_stella import SQL_DB_Stella  # Import from new module

# Load environment variables from .env file
load_dotenv()
apiKEY = os.getenv("API_KEY")
db_user = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT",3306)
db_host = os.getenv("DB_HOST", "127.0.0.1")

# Validate environment variables
required_env_vars = {
    "API_KEY": apiKEY,
    "DB_USERNAME": db_user,
    "DB_PASSWORD": db_password,
    "DB_NAME": db_name
}
for var_name, var_value in required_env_vars.items():
    if not var_value:
        raise ValueError(f"{var_name} not found in .env file. Please set it and try again.")

# Define API endpoints
graph_url = f"https://gateway.thegraph.com/api/{apiKEY}/subgraphs/id/LgiKJnsTspbsPBLqDPqULPtnAdSZP6LfPCSo3GWuJ5a"
pools_apr_url = "https://apr-api.stellaswap.com/api/v1/integral/poolsApr"
farming_apr_url = "https://apr-api.stellaswap.com/api/v1/integral/offchain/farmingAPR"

# Function to fetch Pools APR data
def fetch_pools_apr():
    try:
        response = requests.get(pools_apr_url)
        if response.status_code == 200:
            data = response.json()
            if data.get("isSuccess") and "result" in data:
                return data["result"]
            else:
                print("Pools APR API response missing expected data.")
                return {}
        else:
            print(f"Pools APR API request failed with status code {response.status_code}: {response.text}")
            return {}
    except Exception as e:
        print(f"Error fetching Pools APR data: {e}")
        return {}

# Function to fetch Farming APR data
def fetch_farming_apr():
    try:
        response = requests.get(farming_apr_url)
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 200 and "result" in data and "pools" in data["result"]:
                return data["result"]["pools"]
            else:
                print("Farming APR API response missing expected data.")
                return {}
        else:
            print(f"Farming APR API request failed with status code {response.status_code}: {response.text}")
            return {}
    except Exception as e:
        print(f"Error fetching Farming APR data: {e}")
        return {}

# Function to fetch pool data with 24h volume
def fetch_pool_data(timestamp_23h_ago, timestamp_25h_ago):
    query = f"""
    {{
      pools(first: 55) {{
        id
        token0 {{
          id
          symbol
          name
          decimals
        }}
        token1 {{
          id
          symbol
          name
          decimals
        }}
        liquidity
        sqrtPrice
        tick
        volumeUSD
        txCount
        feesUSD
        poolHourData(
          where: {{ periodStartUnix_gte: {timestamp_25h_ago}, periodStartUnix_lte: {timestamp_23h_ago} }}
          orderBy: periodStartUnix
          orderDirection: desc
          first: 1
        ) {{
          periodStartUnix
          feesUSD
          volumeUSD
        }}
      }}
    }}
    """
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(graph_url, json={'query': query}, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Query failed with status code {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching pool data: {e}")
        return None

# Function to fetch position data and calculate token amounts
def fetch_token_amounts(pool_id):
    query = f"""
    {{
      positions(where: {{ pool: "{pool_id.lower()}", liquidity_gt: 0 }}, first: 1000) {{
        id
        tickLower {{
          tickIdx
        }}
        tickUpper {{
          tickIdx
        }}
        liquidity
        pool {{
          tick
          sqrtPrice
          token0 {{
            symbol
            decimals
          }}
          token1 {{
            symbol
            decimals
          }}
        }}
      }}
    }}
    """
    headers = {"Content-Type": "application/json"}
    response = requests.post(graph_url, json={'query': query}, headers=headers)
    
    if response.status_code != 200:
        print(f"Token amount query failed for pool {pool_id}: {response.status_code}")
        return 0, 0
    
    data = response.json()
    positions = data.get('data', {}).get('positions', [])
    if not positions:
        print(f"No active positions found for pool: {pool_id}")
        return 0, 0
    
    pool = positions[0]["pool"]
    sqrt_price_current = int(pool["sqrtPrice"]) / (2 ** 96)
    token0_decimals = int(pool["token0"]["decimals"])
    token1_decimals = int(pool["token1"]["decimals"])
    
    total_amount0, total_amount1 = 0, 0
    
    for pos in positions:
        tick_lower = int(pos["tickLower"]["tickIdx"])
        tick_upper = int(pos["tickUpper"]["tickIdx"])
        liquidity = int(pos["liquidity"])
        
        sqrt_price_low = math.sqrt(1.0001 ** tick_lower)
        sqrt_price_high = math.sqrt(1.0001 ** tick_upper)
        
        amount0, amount1 = calculate_token_amounts(liquidity, sqrt_price_current, sqrt_price_low, sqrt_price_high)
        
        total_amount0 += amount0 / (10 ** token0_decimals)
        total_amount1 += amount1 / (10 ** token1_decimals)
    
    return total_amount0, total_amount1

# Function to calculate token amounts
def calculate_token_amounts(liquidity, sqrt_price_current, sqrt_price_low, sqrt_price_high):
    if sqrt_price_low > sqrt_price_high:
        sqrt_price_low, sqrt_price_high = sqrt_price_high, sqrt_price_low
    
    sqrt_price = max(min(sqrt_price_current, sqrt_price_high), sqrt_price_low)
    
    amount0 = liquidity * (sqrt_price_high - sqrt_price) / (sqrt_price * sqrt_price_high) if sqrt_price_current < sqrt_price_high else 0
    amount1 = liquidity * (sqrt_price - sqrt_price_low) if sqrt_price_current > sqrt_price_low else 0
    
    return amount0, amount1

# Function to process the fetched data
def process_data(data, pools_apr_data, farming_apr_data):
    if not data or 'data' not in data or 'pools' not in data['data']:
        print("No valid data to process.")
        return []
    
    processed_data = []
    pools = data['data']['pools']
    
    for pool in pools:
        pool_id = pool['id']
        amount_token0, amount_token1 = fetch_token_amounts(pool_id)
        
        farming_data = farming_apr_data.get(pool_id, {})
        farming_apr = float(farming_data.get("apr", 0)) if farming_data else 0
        token_rewards = farming_data.get("tokenRewards", {}) if farming_data else {}
        
        pools_apr = float(pools_apr_data.get(pool_id, 0))
        final_apr = pools_apr + farming_apr
        
        pool_data = {
            'pool_id': pool_id,
            'token0_id': pool['token0']['id'],
            'token0_symbol': pool['token0']['symbol'],
            'token0_name': pool['token0']['name'],
            'token0_decimals': int(pool['token0']['decimals']),
            'token1_id': pool['token1']['id'],
            'token1_symbol': pool['token1']['symbol'],
            'token1_name': pool['token1']['name'],
            'token1_decimals': int(pool['token1']['decimals']),
            'liquidity': float(pool['liquidity']),
            'sqrt_price': float(pool['sqrtPrice']),
            'tick': int(pool['tick']),
            'volume_usd_current': float(pool['volumeUSD']),
            'volume_usd_24h_ago': None,
            'volume_usd_24h': None,
            'tx_count': int(pool['txCount']),
            'fees_usd_current': float(pool['feesUSD']),
            'fees_usd_24h_ago': None,
            'fees_usd_24h': None,
            'amount_token0': amount_token0,
            'amount_token1': amount_token1,
            'pools_apr': pools_apr,
            'farming_apr': farming_apr,
            'final_apr': final_apr,
            'token_rewards': str(token_rewards),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if pool.get('poolHourData') and len(pool['poolHourData']) > 0:
            hour_data = pool['poolHourData'][0]
            fees_usd_24h_ago = float(hour_data['feesUSD'])
            pool_data['fees_usd_24h_ago'] = fees_usd_24h_ago
            pool_data['fees_usd_24h'] = pool_data['fees_usd_current'] - fees_usd_24h_ago
            
            volume_usd_24h_ago = float(hour_data['volumeUSD'])
            pool_data['volume_usd_24h_ago'] = volume_usd_24h_ago
            pool_data['volume_usd_24h'] = pool_data['volume_usd_current'] - volume_usd_24h_ago
        
        processed_data.append(pool_data)
    
    return processed_data

# Main execution
def main():
    sql_db = SQL_DB_Stella(
        userName=db_user,
        passWord=db_password,
        dataBase=db_name,
        db_port=db_port,
        host=db_host,
        initializeTable=True
    )
    
    try:
        while True:
            print("\nFetching data...")
            current_timestamp = int(datetime.utcnow().timestamp())
            timestamp_23h_ago = current_timestamp - (23 * 60 * 60)
            timestamp_25h_ago = current_timestamp - (25 * 60 * 60)
            
            batch_id = int(time.time())
            
            pools_apr_data = fetch_pools_apr()
            farming_apr_data = fetch_farming_apr()
            raw_data = fetch_pool_data(timestamp_23h_ago, timestamp_25h_ago)
            
            if raw_data:
                processed_data = process_data(raw_data, pools_apr_data, farming_apr_data)
                
                for pool in processed_data:
                    print(f"Pool {pool['pool_id']}:")
                    print(f"  Token0: {pool['token0_symbol']} - {pool['amount_token0']:.6f}")
                    print(f"  Token1: {pool['token1_symbol']} - {pool['amount_token1']:.6f}")
                    print(f"  24h Fees USD: {pool['fees_usd_24h']}")
                    print(f"  24h Volume USD: {pool['volume_usd_24h']}")
                    print(f"  Final APR: {pool['final_apr']}%")
                    print("---")
                
                sql_db.update_pool_database(processed_data, batch_id)
            
            print("\nSleeping for 1 hour...")
            time.sleep(3600)
    
    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
