import datetime
import uuid

from airflow import decorators
from airflow.models import param

from skypilot_provider import operators

default_args = {
    "owner": "airflow",
    "retries": 1,
}


@decorators.task
def generate_bucket_uuid():
    """Generate a unique bucket UUID for this DAG run."""
    return str(uuid.uuid4())[:4]


@decorators.dag(
    default_args=default_args,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    tags=["skypilot"],
    params={
        'data_bucket_store_type':
        param.Param(
            's3',
            enum=['s3', 'gcs'],
            description='Whether to use S3 or GCS for the data buckets'),
    })
def sky_train():
    bucket_uuid = generate_bucket_uuid()

    common_envs = {
        'DATA_BUCKET_NAME':
        f'sky-data-demo-{bucket_uuid}',
        'DATA_BUCKET_STORE_TYPE':
        '{{ dag_run.conf.get("data_bucket_store_type", params.data_bucket_store_type) }}',
    }

    preprocess_task = operators.SkyPilotClusterOperator(
        task_id='data_preprocess',
        yaml_file=
        'https://raw.githubusercontent.com/skypilot-org/mock-train-workflow/refs/heads/main/data_preprocessing.yaml',
        envs_override=common_envs,
    )
    train_task = operators.SkyPilotClusterOperator(
        task_id='train',
        yaml_file=
        'https://raw.githubusercontent.com/skypilot-org/mock-train-workflow/refs/heads/main/train.yaml',
        envs_override=common_envs,
    )
    eval_task = operators.SkyPilotClusterOperator(
        task_id='eval',
        yaml_file=
        'https://raw.githubusercontent.com/skypilot-org/mock-train-workflow/refs/heads/main/eval.yaml',
        envs_override=common_envs,
    )

    bucket_uuid >> preprocess_task >> train_task >> eval_task


sky_train()
