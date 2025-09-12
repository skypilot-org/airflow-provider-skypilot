import datetime

from airflow import decorators

from skypilot_provider import operators

default_args = {
    "owner": "airflow",
    "retries": 1,
}


@decorators.dag(default_args=default_args,
                start_date=datetime.datetime(2025, 1, 1),
                catchup=False,
                tags=["skypilot"])
def sky_hello_local():
    hello_task = operators.SkyPilotClusterOperator(
        task_id="hello_task",
        # If running locally, copy the examples/ directory into
        # the airflow-worker container:
        # docker cp ./examples/ airflow-worker:/opt/airflow/
        yaml_file="/opt/airflow/examples/hello_world_local/task.sky.yaml",
    )

    hello_task


sky_hello_local()
