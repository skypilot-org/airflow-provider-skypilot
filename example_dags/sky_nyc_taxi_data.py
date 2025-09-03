import datetime
import uuid

from airflow import decorators
from airflow.models import param
from airflow.providers.amazon.aws.hooks import s3
from airflow.providers.google.cloud.hooks import gcs

from skypilot_provider import operators

YEARS = [year for year in range(2015, 2026)]

default_args = {
    "owner": "airflow",
    "retries": 1,
}


@decorators.task
def generate_bucket_uuid():
    """Generate a unique bucket UUID for this DAG run."""
    return str(uuid.uuid4())[:4]


@decorators.task
def create_parallel_task_configs(bucket_uuid: str):
    """Create task configurations with the actual bucket UUID."""
    return [
        {
            'envs_override': {
                'BUCKET_NAME':
                f'sky-data-demo-{bucket_uuid}',
                'BUCKET_STORE_TYPE':
                '{{ dag_run.conf.get("data_bucket_store_type", params.data_bucket_store_type) }}',
                # Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
                'DATA_URLS':
                ','.join([
                    f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
                    for month in range(1, 13)
                ]),
                'YEAR':
                year,
            }
        } for year in YEARS
    ]


@decorators.task
def calculate_average(bucket_name, storage_type='s3'):
    """Read all temporary files from cloud storage (S3 or GCS) and calculate the overall average."""
    if storage_type.lower() == 's3':
        s3_hook = s3.S3Hook(aws_conn_id='skypilot_aws_task')
        file_keys = s3_hook.list_keys(bucket_name=bucket_name, prefix='tmp_')
        if not file_keys:
            raise ValueError(
                f"No result files found in S3 bucket {bucket_name}")
        for file_key in file_keys:
            print(f"Processing file: {file_key}")
            content = s3_hook.read_key(key=file_key, bucket_name=bucket_name)
            total_sum, total_count = _process_file_content(content, file_key)

    elif storage_type.lower() == 'gcs':
        gcs_hook = gcs.GCSHook(gcp_conn_id='skypilot_gcp_task')
        file_keys = gcs_hook.list(bucket_name=bucket_name, prefix='tmp_')
        if not file_keys:
            raise ValueError(
                f"No result files found in GCS bucket {bucket_name}")
        for file_key in file_keys:
            print(f"Processing file: {file_key}")
            content = gcs_hook.download(bucket_name=bucket_name,
                                        object_name=file_key)
            total_sum, total_count = _process_file_content(
                content.decode('utf-8'), file_key)
    else:
        raise ValueError(
            f"Unsupported storage type: {storage_type}. Supported types are 's3' and 'gcs'"
        )

    overall_average = total_sum / total_count

    print(f"=== FINAL RESULTS ===")
    print(f"Total sum: ${total_sum:,.3f}")
    print(f"Total count: {total_count:,}")
    print(f"Overall average: ${overall_average:.3f}")

    return {
        'total_sum': total_sum,
        'total_count': total_count,
        'overall_average': overall_average
    }


def _process_file_content(content, file_key):
    """Helper function to process file content and extract sum/count data."""
    lines = content.strip().split('\n')
    if len(lines) < 2:
        print(f"File {file_key} doesn't have data line, skipping")
        return 0, 0
    total_sum = 0
    total_count = 0
    try:
        data_line = lines[1]
        sum_amount, count_amount = data_line.split(',')
        total_sum += float(sum_amount)
        total_count += int(count_amount)
        print(f"  Sum: {sum_amount}, Count: {count_amount}")
    except ValueError as e:
        print(f"Error parsing file {file_key}: {e}")
    return total_sum, total_count


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
def sky_nyc_taxi_data():
    bucket_uuid = generate_bucket_uuid()
    task_configs = create_parallel_task_configs(bucket_uuid)

    # Process each year's data in parallel
    nyc_taxi_data_tasks = operators.SkyPilotClusterOperator.partial(
        task_id="nyc_taxi_data",
        yaml_file=
        "https://raw.githubusercontent.com/skypilot-org/airflow-provider-skypilot/refs/heads/examples/example_skypilot_yamls/nyc_taxi_data.sky.yaml",
        retry_delay=datetime.timedelta(seconds=10),
    ).expand_kwargs(task_configs)

    # Calculate overall average after all parallel tasks complete
    calculate_avg_task = calculate_average(
        bucket_name=f'sky-data-demo-{bucket_uuid}',
        storage_type=
        '{{ dag_run.conf.get("data_bucket_store_type", params.data_bucket_store_type) }}'
    )

    bucket_uuid >> task_configs >> nyc_taxi_data_tasks >> calculate_avg_task


sky_nyc_taxi_data()
