# Running Example DAGs

This directory contains several example Airflow DAGs to demonstrate how to use the
`SkyPilotClusterOperator`.

## Table of Contents
* [Overview](#overview)
  + [Example 1: NYC Taxi Data Processing Pipeline](#example-1-nyc-taxi-data-processing-pipeline)
  + [Example 2: Machine Learning Training Pipeline](#example-2-machine-learning-training-pipeline)
  + [Example 3: Simple Hello World](#example-3-simple-hello-world)
  + [Example 4: Local Hello World](#example-4-local-hello-world)
  + [Example 5: AWS Credentials Integration](#example-5-aws-credentials-integration)
  + [Example 6: GCP Credentials Integration](#example-6-gcp-credentials-integration)
* [Running the Examples](#running-the-examples)
* [(Optional) Setting up Airflow locally using a custom image](#-optional--setting-up-airflow-locally-using-a-custom-image)
* [Triggering the DAG](#triggering-the-dag)
* [(Optional) Cleaning up the Docker Compose environment](#-optional--cleaning-up-the-docker-compose-environment)
* [References](#references)

## Overview

### Example 1: NYC Taxi Data Processing Pipeline

[sky_nyc_taxi_data.py](sky_nyc_taxi_data.py) includes the definition of the DAG. This example demonstrates a large-scale data processing workflow that processes NYC taxi trip data from 2009-2025. It involves the following stages:

1. **Preprocessing Stage**: Generates a unique bucket UUID and creates parallel task configurations for processing multiple years of data in parallel
2. **Parallel Data Processing Stage**: In parallel, downloads NYC taxi trip data (parquet files) from the NYC TLC website for each year, processes the data using DuckDB to calculate sum and count of total amounts, and stores intermediate results in cloud storage (S3 or GCS)
3. **Data Aggregation Stage**: Reads all intermediate results from cloud storage and calculates the overall average taxi fare across all years and trips

### Example 2: Machine Learning Training Pipeline

[sky_train.py](sky_train.py) includes the definition of the DAG. This example demonstrates a complete mock ML training workflow. It involves the following stages:

1. **Data Preprocessing Stage**: Prepares and cleans raw data for training using a dedicated preprocessing task
2. **Model Training Stage**: Trains a machine learning model using the preprocessed data
3. **Model Evaluation Stage**: Evaluates the trained model's performance and generates metrics

### Example 3: Simple Hello World

[sky_hello.py](sky_hello.py) includes the definition of the DAG. This is the simplest example that demonstrates basic SkyPilot integration. It involves the following stage:

1. **Hello Task**: Executes a simple `echo "Hello, SkyPilot!"` command and displays the conda environment list to verify the cluster setup

### Example 4: Local Hello World

[sky_hello_local.py](sky_hello_local.py) includes the definition of the DAG. This example is similar to the simple hello world but uses local YAML files instead of remote ones. It involves the following stage:

1. **Hello Task**: Executes a simple `echo "Hello, SkyPilot!"` command using a locally mounted YAML file

### Example 5: AWS Credentials Integration

[sky_aws_credentials.py](sky_aws_credentials.py) includes the definition of the DAG. This example demonstrates how to use AWS credentials stored in Airflow connections with SkyPilot tasks. It involves the following stage:

1. **AWS Integration Task**: Sets up AWS CLI, authenticates using provided credentials, and displays the current AWS identity to verify proper credential configuration

### Example 6: GCP Credentials Integration

[sky_gcp_credentials.py](sky_gcp_credentials.py) includes the definition of the DAG. This example demonstrates how to use Google Cloud Platform credentials stored in Airflow connections with SkyPilot tasks. It involves the following stage:

1. **GCP Integration Task**: Sets up Google Cloud SDK, authenticates using provided credentials, and displays the current GCP identity to verify proper credential configuration

## Running the Examples

To run these examples, you need:
1. A SkyPilot remote API server (see [Configuration and Usage](../README.md#configuration-and-usage))
2. An Airflow deployment
3. An extended Airflow image which has `airflow-provider-skypilot` installed (along with other
additional dependencies you may have). Refer to our [Dockerfile](../Dockerfile) for how to create the extended image.

If you already have an existing Airflow deployment, and have added `airflow-provider-skypilot` to your custom Airflow image, you can jump ahead to [Triggering the DAG](#triggering-the-dag).

## (Optional) Setting up Airflow locally using a custom image

The simplest way to run Airflow locally is by using [Docker](https://docs.docker.com/get-started/) and
[Docker Compose](https://docs.docker.com/get-started/workshop/08_using_compose/). These are the steps
based on Airflow's official [guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

1. Clone this repository:
    ```bash
    git clone git@github.com:skypilot-org/airflow-provider-skypilot.git
    cd airflow-provider-skypilot
    ```
2. Fetch `docker-compose.yaml`:
    ```bash
    curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.0/docker-compose.yaml'
    ```
3. Modify `docker-compose.yaml` to use our custom [Dockerfile](../Dockerfile) which has
`airflow-provider-skypilot` installed:

    Comment out the `image: ...` line and remove comment from the `build: .` line in the `docker-compose.yaml` file. The relevant part of the docker-compose file of yours should look similar to:
    ```yaml
    x-airflow-common:
        &airflow-common
        # In order to add custom dependencies or upgrade provider distributions you can use your extended image.
        # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
        # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
        # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.0.0}
        build: .
    ```
4. Run the Docker Compose
    ```bash
    # Build the custom image
    docker compose build
    # One-time DB initialization
    docker compose up airflow-init
    # Start all services
    docker compose up
    ```

5. The Airflow API server should now be available at http://localhost:8080. By default, the account created has the username `airflow` and the password `airflow`.

<p align="center">
    <img alt="Airflow login page" src="https://i.imgur.com/PVIgNBc.png" width="720">
</p>

## Triggering the DAG

Once you have Airflow running and `airflow-provider-skypilot` installed, you can do the following
to trigger the DAG run:

1. Go to the DAGs [page](http://localhost:8080/dags?tags=skypilot) and filter with `tags=skypilot`

<p align="center">
    <img alt="Airflow DAGs page" src="https://i.imgur.com/xpdyDre.png" width="720">
</p>

2. Press the <span>&#9654;</span> (trigger) button on the right

3. Some example DAGs which interact with object storage allow you to choose between using S3 or GCS

<p align="center">
    <img alt="Airflow DAG trigger modal" src="https://i.imgur.com/Fs6IcVl.png" width="720">
</p>

4. Click on "Trigger"

5. Click on the DAG name to go to the detailed page, for example http://localhost:8080/dags/sky_nyc_taxi_data

<p align="center">
    <img alt="Airflow DAG detail page" src="https://i.imgur.com/uhbwy2S.png" width="720">
</p>
<p align="center">
    <img alt="Airflow DAG run page" src="https://i.imgur.com/KzEQflK.png" width="720">
</p>

## (Optional) Cleaning up the Docker Compose environment

Run `docker compose down --volumes --remove-orphans` from the same directory as before.

## References
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
