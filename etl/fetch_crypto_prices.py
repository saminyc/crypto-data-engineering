import json
import requests
import pandas as pd
import boto3
from datetime import datetime, timezone

# Bucket name where data will be stored
BUCKET_NAME = "crypto-data-lake-chowdhury"

# Fetch crypto prices from CoinGecko API
def fetch_prices() -> dict:
    """Fetch crypto prices from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
    "ids": "bitcoin,ethereum,solana,cardano,polkadot",
    "vs_currencies": "usd"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Save raw JSON response to S3
def save_raw_to_s3(raw_data: dict, s3_client):
    """Save raw API response to S3."""
    today = datetime.now(timezone.utc).date()
    key = f"raw/crypto_prices_{today}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(raw_data)
    )

# Transform raw data into pandas DataFrame
def transform_prices(raw_data: dict) -> pd.DataFrame:
    """Transform raw JSON into a pandas DataFrame."""
    # Convert dict-of-dicts into DataFrame
    df = pd.DataFrame(raw_data).T
    # Rename column
    df = df.rename(columns={"usd": "price_usd"})
    # Reset index so coin name becomes a column
    df = df.reset_index().rename(columns={"index": "coin"})
    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    return df

# Save processed DataFrame as partitioned Parquet to S3
def save_processed_to_s3(df: pd.DataFrame, s3_client):
    """Save processed data as partitioned Parquet to S3."""
    now = datetime.now(timezone.utc)
    local_file = "crypto_prices.parquet"
    df.to_parquet(local_file, index=False)
    s3_key = (
        f"processed/year={now.year}/month={now.month}/day={now.day}/"
        f"{local_file}"
    )
    s3_client.upload_file(local_file, BUCKET_NAME, s3_key) # upload to s3

# Main ETL function
def main():
    s3_client = boto3.client("s3")
    raw_data = fetch_prices()
    save_raw_to_s3(raw_data, s3_client)
    transformed_data = transform_prices(raw_data) # transform data
    save_processed_to_s3(transformed_data, s3_client)
    print("ETL completed successfully")
   
# Run the ETL process
if __name__ == "__main__":
    main()