# ML Training Pipeline

This example demonstrates a complete mock ML training workflow. It involves the following stages:

1. Data Preprocessing Stage: Prepares and cleans raw data for training using a dedicated preprocessing task
2. Model Training Stage: Trains a machine learning model using the preprocessed data
3. Model Evaluation Stage: Evaluates the trained model's performance and generates metrics

## Prerequisites
- The SkyPilot API server must have access to either AWS S3 or Google Cloud Storage. Refer to [Cloud Accounts and Permissions](https://docs.skypilot.co/en/latest/cloud-setup/cloud-permissions/index.html) for more details.

## Parameters
- `data_bucket_store_type`: `s3` or `gcs`
