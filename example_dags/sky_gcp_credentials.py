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
def sky_gcp_credentials():
    gcp_task = operators.SkyPilotClusterOperator(
        task_id="gcp_task",
        yaml_file=
        "https://raw.githubusercontent.com/skypilot-org/airflow-provider-skypilot/init/example_skypilot_yamls/gcp.sky.yaml",
        credentials_override={"gcp": "skypilot_gcp_task"})

    gcp_task


sky_gcp_credentials()
