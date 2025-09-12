## NYC Taxi Data Processing Pipeline

This example demonstrates a large-scale data processing workflow that processes NYC taxi trip data from 2009-2025. It involves the following stages:

1. **Preprocessing Stage**: Generates a unique bucket UUID and creates parallel task configurations for processing multiple years of data in parallel
2. **Parallel Data Processing Stage**: In parallel, downloads NYC taxi trip data (parquet files) from the NYC TLC website for each year, processes the data using DuckDB to calculate sum and count of total amounts, and stores intermediate results in cloud storage (S3 or GCS)
3. **Data Aggregation Stage**: Reads all intermediate results from cloud storage and calculates the overall average taxi fare across all years and trips

## Prerequisites
- The SkyPilot API server must have access to either AWS S3 or Google Cloud Storage. Refer to [Cloud Accounts and Permissions](https://docs.skypilot.co/en/latest/cloud-setup/cloud-permissions/index.html) for more details.

## Parameters
- `data_bucket_store_type`: `s3` or `gcs`
