import pendulum
from airflow import decorators

from skypilot_provider import operators

default_args = {
    "owner": "airflow",
    "retries": 1,
}


@decorators.dag(default_args=default_args,
                start_date=pendulum.datetime(2023, 1, 1),
                catchup=False,
                tags=["skypilot"])
def sky_hello_local():
    hello_task = operators.SkyPilotClusterOperator(
        task_id="hello_task",
        # If running locally, copy the example_skypilot_yamls/ directory into
        # the airflow-worker container:
        # docker compose cp ./example_skypilot_yamls/ airflow-worker:/opt/airflow/
        base_path="/opt/airflow",
        yaml_path="example_skypilot_yamls/hello.sky.yaml",
    )

    hello_task


sky_hello_local()
