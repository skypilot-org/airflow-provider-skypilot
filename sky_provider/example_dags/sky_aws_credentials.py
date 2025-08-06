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
def sky_aws_credentials():
    aws_task = SkyTaskOperator(
        task_id="aws_task",
        base_path=
        "https://github.com/skypilot-org/airflow-provider-skypilot.git",
        git_branch=
        "init",  # TODO: remove this before merging/once first PR is merged
        yaml_path="example_task_yamls/aws.sky.yaml",
        credentials_override={"aws": "skypilot_aws_task"})

    aws_task


sky_aws_credentials()
