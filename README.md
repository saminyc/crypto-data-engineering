# Crypto Price Data Engineering Pipeline

## Overview
This project is an end-to-end data engineering pipeline that ingests cryptocurrency price data from a public API (CoinGecko API), stores raw and processed data in Amazon S3, and enables SQL-based analytics using Amazon Athena.

The goal of the project is to demonstrate core data engineering concepts such as API ingestion, data lake design, Parquet-based storage, partitioning, and serverless analytics on AWS.

---

## Architecture

CoinGecko API  
→ Python ETL  
→ Amazon S3 (raw JSON)  
→ Data transformation (pandas)  
→ Amazon S3 (processed Parquet, partitioned)  
→ Amazon Athena (SQL analytics)

---

## Tech Stack

- **Languages:** Python, SQL  
- **Cloud:** AWS (S3, Athena)  
- **Libraries:** requests, pandas, boto3  
- **Storage Formats:** JSON, Parquet  

---

## Data Flow

1. Fetch cryptocurrency prices from the CoinGecko public API.
2. Store the raw API response as JSON in Amazon S3 (raw layer).
3. Transform the raw data into an analytics-friendly tabular format.
4. Write processed data as Parquet files partitioned by year, month, and day.
5. Query the data directly from S3 using Athena external tables.

---
