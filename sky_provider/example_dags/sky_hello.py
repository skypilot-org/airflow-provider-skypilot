from airflow.decorators import dag
from pendulum import datetime

from sky_provider.operators import SkyTaskOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
}


@dag(default_args=default_args,
     start_date=datetime(2023, 1, 1),
     catchup=False,
     tags=["skypilot"])
def sky_hello():
    hello_task = SkyTaskOperator(
        task_id="hello_task",
        base_path=
        "https://github.com/skypilot-org/airflow-provider-skypilot.git",
        git_branch=
        "init",  # TODO: remove this before merging/once first PR is merged
        yaml_path="example_skypilot_yamls/hello.sky.yaml",
    )

    hello_task


sky_hello()
