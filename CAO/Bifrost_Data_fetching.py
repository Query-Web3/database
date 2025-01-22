import requests
import pandas as pd
import time

from SQL_DB import SQL_DB

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
        print(f"Failed to fetch data. Status code: {response.status_code}")

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
        print(f"Failed to fetch data. Status code: {response.status_code}")    

def main():

    sqlDB = SQL_DB(userName = 'queryweb3', passWord = '!SDCsdin2df2@', dataBase = 'QUERYWEB3', initializeTable=True)  # connect to the database

    while True:
        print("\nFetching data...")
        try:
            data_frames1 = fetch_data()
        except:
            print("Warning, fetching site API error, try again later")
            continue
        
        try:
            data_frames2 = fetch_data2()
        except:
            print("Warning, fetching staking API error, try again later")
            continue 
        
        # print(data_frames1)

        # print("+++++++++++++")
        # print(data_frames2)

        batch_id = int(time.time())  # Current timestamp in seconds
        sqlDB.update_bifrost_database(data_frames1,data_frames2,batch_id)

        print("\nSleeping for 1 hour...")
        time.sleep(3600)

if __name__ == "__main__":
    main()
