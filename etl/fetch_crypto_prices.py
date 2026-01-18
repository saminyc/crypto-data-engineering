import json
import requests
import pandas as pd
import boto3
from datetime import datetime, timezone


BUCKET_NAME = "crypto-data-lake-chowdhury"


def fetch_prices() -> dict:
    """Fetch crypto prices from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def save_raw_to_s3(raw_data: dict, s3_client):
    """Save raw API response to S3."""
    today = datetime.now(timezone.utc).date()
    key = f"raw/crypto_prices_{today}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(raw_data)
    )


def transform_prices(raw_data: dict) -> pd.DataFrame:
    """Transform raw JSON into a pandas DataFrame."""
    df = pd.json_normalize(raw_data)
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    return df


def save_processed_to_s3(df: pd.DataFrame, s3_client):
    """Save processed data as partitioned Parquet to S3."""
    now = datetime.now(timezone.utc)

    local_file = "crypto_prices.parquet"
    df.to_parquet(local_file, index=False)

    s3_key = (
        f"processed/year={now.year}/month={now.month}/day={now.day}/"
        "crypto_prices.parquet"
    )

    s3_client.upload_file(local_file, BUCKET_NAME, s3_key)


def main():
    s3_client = boto3.client("s3")

    raw_data = fetch_prices()
    save_raw_to_s3(raw_data, s3_client)

    transformed_data = transform_prices(raw_data)
    save_processed_to_s3(transformed_data, s3_client)

    print("ETL completed successfully")


if __name__ == "__main__":
    main()
