# Running Example DAGs

This directory contains several example Airflow DAGs to demonstrate how to use the
`SkyPilotClusterOperator`.

To run these examples, you need:
1. A SkyPilot remote API server (see [Configuration and Usage](../README.md#configuration-and-usage))
2. An Airflow deployment
3. An extended Airflow image which has `airflow-provider-skypilot` installed (along with other
additional dependencies you may have)

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

<img alt="Airflow login page" src="https://i.imgur.com/PVIgNBc.png" width="720">

## Triggering the DAG

Once you have Airflow running and `airflow-provider-skypilot` installed, you can do the following
to trigger the DAG run:

1. Go to the DAGs [page](http://localhost:8080/dags?tags=skypilot) and filter with `tags=skypilot`

<img alt="Airflow DAGs page" src="https://i.imgur.com/HvZbPlF.png" width="720">

2. Press the <span>&#9654;</span> (trigger) button on the right

3. Some example DAGs which interact with object storage allow you to choose between using S3 or GCS

<img alt="Airflow DAG trigger modal" src="https://i.imgur.com/Fs6IcVl.png" width="720">

4. Click on "Trigger"

5. Click on the DAG name to go to the detailed page, for example http://localhost:8080/dags/sky_nyc_taxi_data

<img alt="Airflow DAG detail page" src="https://i.imgur.com/uhbwy2S.png" width="720">
<img alt="Airflow DAG run page" src="https://i.imgur.com/KzEQflK.png" width="720">

## (Optional) Cleaning up the Docker Compose environment

Run `docker compose down --volumes --remove-orphans` from the same directory as before.

## References
- https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
