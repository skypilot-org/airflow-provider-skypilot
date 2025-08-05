# airflow-provider-skypilot
SkyPilot provider for Apache Airflow

## Development setup
To avoid package conflicts, create and activate a clean conda environment:
```bash
# SkyPilot requires 3.7 <= python <= 3.11.
conda create -y -n sky python=3.10
conda activate sky
```

### Create a conda environment

To avoid package conflicts, create and activate a clean conda environment:
```bash
conda create -y -n airflow-provider-sky python=3.10
conda activate airflow-provider-sky
```

### Install Dependencies

```bash
pip install .
# Install development dependencies
pip install -r requirements-dev.txt
```
