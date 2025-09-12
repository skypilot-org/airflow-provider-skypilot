ARG AIRFLOW_VERSION=3.0.0
FROM apache/airflow:${AIRFLOW_VERSION}

# Install our provider
USER root
RUN apt-get update && apt-get install -y git && apt-get clean
USER airflow
# TODO: Install from PyPI once released
RUN pip install git+https://github.com/skypilot-org/airflow-provider-skypilot.git@v0.1.1
# Or if building locally:
# COPY --chown=airflow:root . /opt/airflow/providers/airflow-provider-skypilot/
# RUN pip install -e /opt/airflow/providers/airflow-provider-skypilot/

# Copy example DAGs to Airflow's DAGs directory
COPY --chown=airflow:root example_dags/ /opt/airflow/dags/
