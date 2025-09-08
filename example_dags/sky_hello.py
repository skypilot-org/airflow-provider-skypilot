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
def sky_hello():
    hello_task = operators.SkyPilotClusterOperator(
        task_id="hello_task",
        name="mycluster",
        yaml_file=
        "https://raw.githubusercontent.com/skypilot-org/airflow-provider-skypilot/refs/heads/master/example_skypilot_yamls/hello.sky.yaml",
    )

    hello_task


sky_hello()
