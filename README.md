<p align="center">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/skypilot-wide-dark-1k.png">
        <img alt="SkyPilot" src="https://raw.githubusercontent.com/skypilot-org/skypilot/master/docs/source/images/skypilot-wide-light-1k.png" width=240>
    </picture>
</p>
<h1 align="center">
  Apache Airflow Provider for SkyPilot
</h1>
  <h3 align="center">
  A provider you can install into your Airflow environment to run SkyPilot tasks.
</h3>
<br/>

## Operators

This provider includes the `SkyPilotClusterOperator` which creates SkyPilot clusters, runs the task
defined in a SkyPilot YAML (local file or remote URL), and terminates the cluster upon completion.
It integrates with Airflow connections so that you can use your existing credentials
with your SkyPilot tasks.

More operators like `SkyJobOperator` are in the roadmap, so stay tuned for that as well.

## Installation

You can install this package on top of an existing Airflow deployment via `pip install airflow-provider-skypilot`. For the minimum Airflow version supported, see [Requirements](#requirements) below.

You should be able to see `airflow-provider-skypilot` on the Providers page
upon successful installation.

<p align="center">
    <img alt="Airflow variables" src="https://i.imgur.com/YFSdzKz.png" width="720">
</p>

## Configuration and Usage

1. Deploy a SkyPilot [remote API Server](https://docs.skypilot.co/en/latest/reference/api-server/api-server.html#remote-api-server-multi-user-teams)

2. Set the `SKYPILOT_API_SERVER_ENDPOINT` variable in Airflow to point to your remote API Server endpoint

    <p align="center">
        <img alt="Airflow variables" src="https://i.imgur.com/rr7SfFP.png" width="720">
    </p>

3. Import `SkyPilotClusterOperator`, and use it in your Airflow DAG.

    ```python
    from skypilot_provider.operators import SkyPilotClusterOperator

    ...

    @dag(
        default_args=default_args,
        tags=["skypilot"],
    )
    def sky_training_workflow():
        bucket_uuid = generate_bucket_uuid()

        env_vars = {
            "DATA_BUCKET_NAME": f"sky-data-demo-{bucket_uuid}",
            "DATA_BUCKET_STORE_TYPE": "s3",
        }

        preprocess_task = SkyPilotClusterOperator(
            task_id="preprocess",
            yaml_file="https://raw.githubusercontent.com/skypilot-org/mock-train-workflow/refs/heads/main/data_preprocessing.yaml",
            env_vars=env_vars,
        )

        train_task = SkyPilotClusterOperator(
            task_id="train",
            yaml_file="https://raw.githubusercontent.com/skypilot-org/mock-train-workflow/refs/heads/main/train.yaml",
            env_vars=env_vars,
        )

        eval_task = SkyPilotClusterOperator(
            task_id="eval",
            yaml_file="https://raw.githubusercontent.com/skypilot-org/mock-train-workflow/refs/heads/main/eval.yaml",
            env_vars=env_vars,
        )

        # Define the workflow
        bucket_uuid >> preprocess_task >> train_task >> eval_task

    sky_training_workflow()
    ```

See `example_dags/` for more examples.

## Managing SkyPilot Version

All operators supports both stable and nightly versions of SkyPilot.

- **Default**: Uses the latest stable release
  ```python
  SkyPilotClusterOperator(
      task_id="my_task",  # skypilot[all] (latest stable)
      ...
  )
  ```

- **Stable versions**
  ```python
  SkyPilotClusterOperator(
      task_id="my_task",
      skypilot_version="0.10.0",  # skypilot[all]==0.10.0
      ...
  )
  ```

- **Nightly versions**
  ```python
  SkyPilotClusterOperator(
      task_id="my_task",
      skypilot_version="1.0.0.dev20250806",  # skypilot-nightly[all]==1.0.0.dev20250806
      ...
  )
  ```

The operator automatically detects nightly versions by checking for "dev" in the version string.

## Optional: Accessing private resources from remote clusters

If you have resources that is not accessible with the cloud credentials on the API server,
you can use a different cloud credential to grant the remote clusters created by the operator access to those resources.

1. Create connections in Airflow to store your cloud credentials. Today, we support AWS and GCP connections.
    <p align="center">
        <img alt="Airflow connections" src="https://i.imgur.com/9VbD44X.png" width="720">
    </p>
    <p align="center">
        <img alt="Airflow GCP connection" src="https://i.imgur.com/meHEw8w.png" width="720">
    </p>

2. Use the connection in the operator:

    ```python
    SkyPilotClusterOperator(
        task_id="my_task",
        credentials_override={"gcp": "skypilot_gcp_task"},
        ...
    )
    ```


## Requirements

The minimum Apache Airflow version supported by this provider distribution is ``2.10.0``.

| PIP package                                 | Version required |
|---------------------------------------------|------------------|
| ``apache-airflow``                          | ``>=2.10.0``     |
| ``apache-airflow-providers-google``         | ``>=10.0.0``     |
| ``apache-airflow-providers-amazon``         | ``>=8.0.0``      |
| ``apache-airflow-providers-standard``       |                  |

Note: The `skypilot` package is not included here, as it is used within a virtual environment,
using [PythonVirtualenvOperator](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html#pythonvirtualenvoperator), so that it does not conflict with `apache-airflow` on the main Python environment.

## Development setup

### Create a conda environment

To avoid package conflicts, create and activate a clean conda environment:
```bash
conda create -y -n airflow-provider-skypilot python=3.10
conda activate airflow-provider-skypilot
```

### Install Dependencies

```bash
pip install .
# Install development dependencies
pip install -r requirements-dev.txt
```
