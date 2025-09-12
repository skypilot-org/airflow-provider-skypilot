# Hello World (Local)

This example is the same as [Hello World](../hello_world/README.md), but uses a local YAML file instead of a remote URL.

## Prerequisites
- Copy the `examples/` directory to the Airflow worker container. For example, if you're running locally with Docker:
  ```bash
  docker cp ./examples/ airflow-worker:/opt/airflow/
  ```
