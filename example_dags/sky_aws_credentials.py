import datetime

from airflow import decorators

from skypilot_provider import operators

default_args = {
    "owner": "airflow",
    "retries": 1,
}


@decorators.dag(default_args=default_args,
                start_date=datetime.datetime(2025,
                                             1,
                                             1,
                                             tzinfo=datetime.timezone.utc),
                catchup=False,
                tags=["skypilot"])
def sky_aws_credentials():
    aws_task = operators.SkyPilotClusterOperator(
        task_id="aws_task",
        yaml_file=
        "https://raw.githubusercontent.com/skypilot-org/airflow-provider-skypilot/init/example_skypilot_yamls/aws.sky.yaml",
        credentials_override={"aws": "skypilot_aws_task"})

    aws_task


sky_aws_credentials()
