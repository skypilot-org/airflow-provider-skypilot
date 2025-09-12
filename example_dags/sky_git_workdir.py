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
def sky_git_workdir():
    git_workdir_task = operators.SkyPilotClusterOperator(
        task_id="git_workdir_task",
        yaml_file=
        "https://raw.githubusercontent.com/skypilot-org/airflow-provider-skypilot/refs/heads/workdir-examples/example_skypilot_yamls/git_workdir.sky.yaml",
    )

    git_workdir_task


sky_git_workdir()
