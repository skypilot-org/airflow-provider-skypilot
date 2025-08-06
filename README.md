<p align="center">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
    <img alt="SkyPilot" src="https://docs.skypilot.co/en/latest/_images/SkyPilot_wide_light.svg" width="240" />
</p>
<h1 align="center">
  Apache Airflow Provider for SkyPilot
</h1>
  <h3 align="center">
  A provider you can install into your Airflow environment to run SkyPilot tasks.
</h3>
<br/>

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

3. [Optional] Create connections in Airflow to store your cloud credentials. Today, we support AWS and GCP connections.

    <p align="center">
        <img alt="Airflow connections" src="https://i.imgur.com/9VbD44X.png" width="720">
    </p>
    <p align="center">
        <img alt="Airflow GCP connection" src="https://i.imgur.com/meHEw8w.png" width="720">
    </p>

4. Import `SkyTaskOperator`, and use it in your Airflow DAG.

    ```python
    from airflow import DAG

    from sky_provider.operators import SkyTaskOperator

    with DAG(...) as dag:
        ...
        task = SkyTaskOperator(
            task_id="data_preprocess",
            base_path="/opt/airflow", # This can point to a git repository too
            yaml_path="data_preprocessing.sky.yaml",
        )
        ...
    ```

See `sky_provider/example_dags` for more examples.

## Managing SkyPilot Version

All operators supports both stable and nightly versions of SkyPilot.

- **Default**: Uses the latest stable release
  ```python
  SkyTaskOperator(
      task_id="my_task",  # skypilot[all] (latest stable)
      ...
  )
  ```

- **Stable versions**: Use the standard SkyPilot package
  ```python
  SkyTaskOperator(
      task_id="my_task",
      skypilot_version="0.10.0",  # skypilot[all]==0.10.0
      ...
  )
  ```

- **Nightly versions**: Automatically use the nightly package for dev versions
  ```python
  SkyTaskOperator(
      task_id="my_task",
      skypilot_version="1.0.0.dev20250806",  # skypilot-nightly[all]==1.0.0.dev20250806
      ...
  )
  ```

The operator automatically detects nightly versions by checking for "dev" in the version string.

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
