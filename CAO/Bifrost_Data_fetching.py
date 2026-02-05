import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

from SQL_DB import SQL_DB
import numpy as np
from logging_config import logger
from utils import generate_batch_id, DataValidator, LivelinessProbe

def fetch_data():
    # Fetching data from the API
    url = "https://dapi.bifrost.io/api/site"
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Flattening and organizing the data for table representation
        def extract_data(data):
            extracted = []
            for key, value in data.items():
                if isinstance(value, dict):
                    extracted.append({"Asset": key, **value})
                else:
                    extracted.append({"Asset": key, "Value": value})
            return extracted

        # Create DataFrame
        flattened_data = extract_data(data)
        df = pd.DataFrame(flattened_data)

        return df 
    else:
        logger.error(f"Failed to fetch data from site API. Status code: {response.status_code}")

def fetch_data2():
    # fetching data from staking API 
    # Define the URL to fetch data from
    url = "https://dapi.bifrost.io/api/staking"  

    # Fetch data from the API
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the response JSON
        raw_data = response.json()

        # Process the data into the desired structure
        processed_data = {
            "name": "Bifrost",
            "supportedAssets": []
        }

        for asset in raw_data.get("supportedAssets", []):  # Adjust "assets" if the key is different
            asset_data = {
                "contractAddress": asset.get("contractAddress", ""),
                "symbol": asset.get("symbol", ""),
                "slug": asset.get("slug", ""),
                "baseSlug": asset.get("baseSlug", ""),
                "unstakingTime": asset.get("unstakingTime", 0),
                "users": asset.get("users", 0),
                "apr": asset.get("apr", 0),
                "fee": asset.get("fee", 10),
                "price": asset.get("price", 0.0),
                "exchangeRatio": asset.get("exchangeRatio", 1.0),
                "supply": asset.get("supply", 0.0)
            }
            processed_data["supportedAssets"].append(asset_data)
        
        df2 = pd.DataFrame(processed_data["supportedAssets"])
        return df2

        #print(f"Data successfully processed and saved to {processed_data}")
    else:
        logger.error(f"Failed to fetch data from staking API. Status code: {response.status_code}")

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df

    df = df.copy()

    # 1) Normalize common string forms of NaN/inf to real NaN
    df.replace(
        ["NaN", "nan", "NAN", "Inf", "INF", "-Inf", "-INF"],
        np.nan,
        inplace=True
    )

    # 2) Replace numeric infinities with NaN
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 3) OPTIONAL: try to coerce obvious numeric-looking object columns
    for col in df.columns:
        if df[col].dtype == "object":
            # Use try-except instead of deprecated errors='ignore'
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

    # 4) Turn all NaN into None so MySQL connector sends NULL
    df = df.where(pd.notnull(df), None)

    return df

def run_pipeline(db_config=None, single_run=False):
    # Load environment variables from .env file (if present)
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
    
    sqlDB = SQL_DB(db_config=db_config, userName = db_user, passWord = db_password, dataBase = db_name, host=db_host, port = db_port, initializeTable=True)  # connect to the database

    while True:
        logger.info("Fetching data...")
        try:
            data_frames1 = fetch_data()
        except Exception as e:
            logger.warning(f"Warning, fetching site API error, try again later: {e}")
            if single_run: return # Exit on error if single run
            continue
        
        try:
            data_frames2 = fetch_data2()
        except Exception as e:
            logger.warning(f"Warning, fetching staking API error, try again later: {e}")
            if single_run: return # Exit on error if single run
            continue 
        
        df1 =  sanitize_df(data_frames1)
        df2 =  sanitize_df(data_frames2)

        # Compute hash for deduplication
        data_to_hash = {
            "df1": df1.to_dict('records') if df1 is not None else [],
            "df2": df2.to_dict('records') if df2 is not None else []
        }
        current_hash = DataValidator.compute_hash(data_to_hash)
        last_hash = sqlDB.get_last_bifrost_hash()
        
        if current_hash and current_hash == last_hash:
            logger.info("Duplicate data detected (hash matches last batch). Skipping DB update.")
        else:
            batch_id = generate_batch_id()
            sqlDB.update_bifrost_database(df1, df2, batch_id, data_hash=current_hash)

        if single_run:
            logger.info("Single run completed.")
            break

        LivelinessProbe.record_heartbeat("bifrost")
        logger.info("Sleeping for 1 hour...")
        time.sleep(3600)

if __name__ == "__main__":
    run_pipeline()
