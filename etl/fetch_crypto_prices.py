import requests
import pandas as pd
from datetime import datetime, timezone

# Fetching prices from a public cryptocurrency API: CoinGecko
def fetch_prices()->dict:
    url="https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd"
    }
    response=requests.get(url,params=params)
    json_data=response.json() # Convert response to JSON
    response.raise_for_status() # Ensure we raise an error for bad responses
    return json_data # json_data is a list of dictionaries

# Transforming it into tabular format using pandas
def transform_prices(raw_data:dict)->pd.DataFrame:
    """Transform the JSON data into a pandas DataFrame."""
    df=pd.json_normalize(raw_data) # Normalize JSON data into a flat table
    df["ingestion_timestamp"]=datetime.now(timezone.utc) # Adding ingestion timestamp
    return df

# Main function to fetch and transform crypto prices to a parquet file
def main():
    raw_data_fetch=fetch_prices()
    transformed_data=transform_prices(raw_data_fetch)
    print("Data fetched and transformed successfully.")
    transformed_data.to_parquet("crypto_prices.parquet",index=False)
    print("Data saved to crypto_prices.parquet")

if __name__=="__main__":
    main()


