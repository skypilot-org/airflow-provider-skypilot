from typing import Any, Dict, Optional, Sequence

from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import \
    PythonVirtualenvOperator
from packaging.version import Version

if Version(airflow_version) >= Version("3.0.0"):
    from airflow.sdk import Variable
else:
    from airflow.models import Variable

from .credentials_utils import (CREDENTIAL_HANDLERS, ClusterCredentials,
                                CredentialsOverride)


class SkyPilotClusterOperator(PythonVirtualenvOperator):
    """
    Creates a SkyPilot cluster and runs a task,
    as defined in the SkyPilot YAML file.

    Args:
        yaml_file: Path to a local YAML file or an HTTP(S) URL to a remote YAML file.
        name: Optional name for the cluster.
        credentials_override: Credentials override configuration.
        envs_override: Additional environment variables to override.
        skypilot_version: Optional version of SkyPilot to use.
    """

    template_fields: Sequence[str] = (
        'yaml_file',
        'name',
        'credentials_override',
        'envs_override',
        'skypilot_version',
    )

    def __init__(
        self,
        yaml_file: str,
        name: Optional[str] = None,
        credentials_override: Optional[CredentialsOverride] = None,
        envs_override: Optional[Dict[str, str]] = None,
        skypilot_version: Optional[str] = None,
        **kwargs,
    ) -> None:
        if not yaml_file or not str(yaml_file).strip():
            raise ValueError('yaml_file must be a non-empty string')
        if name is not None and not str(name).strip():
            raise ValueError('name must be a non-empty string')

        if skypilot_version is None:
            skypilot_requirement = 'skypilot[all]'
        elif 'dev' in skypilot_version:
            skypilot_requirement = f'skypilot-nightly[all]=={skypilot_version}'
        else:
            skypilot_requirement = f'skypilot[all]=={skypilot_version}'
        super().__init__(
            python_callable=run_sky_task_with_credentials,
            # PythonVirtualenvOperator now uses uv by default if available,
            # so we need to use --pre, as the Azure CLI has an issue with uv.
            pip_install_options=['--pre'],
            # We had an issue with the latest (prerelease) version of httpx,
            # so pinning it to the latest stable version for now.
            requirements=[skypilot_requirement, 'httpx<=0.28.1'],
            python_version='3.11',
            **kwargs,
        )

        self.yaml_file = yaml_file
        self.name = name
        self.credentials_override = credentials_override
        self.envs_override = envs_override or {}
        self.skypilot_version = skypilot_version

    def execute(self, context):
        api_server_endpoint = Variable.get('SKYPILOT_API_SERVER_ENDPOINT')
        if not api_server_endpoint:
            raise AirflowException(
                'SKYPILOT_API_SERVER_ENDPOINT Variable is not set')

        # Get credentials in the main Airflow environment,
        # as it depends on Airflow providers not
        # available later in the virtualenv.
        credentials = self._get_credentials()

        self.op_kwargs = {
            'yaml_file': self.yaml_file,
            'name': self.name,
            'credentials': credentials,
            'envs_override': self.envs_override,
            'api_server_endpoint': api_server_endpoint,
        }
        return super().execute(context)

    def _get_credentials(self) -> ClusterCredentials:
        """
        Get credential artifacts from all configured cloud providers.

        Returns:
            ClusterCredentials with combined file_mounts and env_vars from all providers
        """
        if self.credentials_override is None:
            return {'file_mounts': {}, 'env_vars': {}}

        file_mounts = {}
        env_vars = {}
        for cloud_name, handler_class in CREDENTIAL_HANDLERS.items():
            conn_id = self.credentials_override.get(
                cloud_name)  # type: ignore[literal-required]
            if conn_id:
                handler = handler_class(str(conn_id))
                cluster_credentials = handler.get_cluster_credentials()
                file_mounts.update(cluster_credentials['file_mounts'])
                env_vars.update(cluster_credentials['env_vars'])

        return {'file_mounts': file_mounts, 'env_vars': env_vars}


def run_sky_task_with_credentials(
    yaml_file: str,
    name: Optional[str],
    credentials: ClusterCredentials,
    envs_override: Dict[str, str],
    api_server_endpoint: str,
):
    import os
    import tempfile
    import uuid

    import httpx
    import yaml

    def _launch_from_config(task_config: Dict[str, Any], task_name_hint: str,
                            credentials: ClusterCredentials,
                            envs_override: Dict[str, str]):
        """Launch a SkyPilot task from a config dict with injected credentials."""
        import sky

        if 'envs' not in task_config:
            task_config['envs'] = {}
        if 'file_mounts' not in task_config:
            task_config['file_mounts'] = {}

        task_config['envs'].update(envs_override)
        task_config['envs'].update(credentials['env_vars'])

        # Create temporary files for storing credentials,
        # which will be mounted on the cluster.
        temp_files = []
        try:
            for dst_path, content in credentials['file_mounts'].items():
                temp_file = tempfile.NamedTemporaryFile(mode='w',
                                                        delete=False,
                                                        suffix='.tmp')
                temp_file.write(content)
                temp_file.close()
                temp_files.append(temp_file.name)

                task_config['file_mounts'][dst_path] = temp_file.name

            task = sky.Task.from_yaml_config(task_config)
            cluster_name = (name if name is not None else
                            f'{task_name_hint}-{str(uuid.uuid4())[:4]}')
            print(f'Starting SkyPilot cluster {cluster_name}')
            launch_request_id = sky.launch(task,
                                           cluster_name=cluster_name,
                                           down=True)
            job_id, _ = sky.stream_and_get(launch_request_id)

            # Stream the logs
            sky.tail_logs(cluster_name=cluster_name,
                          job_id=job_id,
                          follow=True)

            # Terminate the cluster after the task is done
            print(f'Terminating SkyPilot cluster {cluster_name}')
            down_id = sky.down(cluster_name)
            sky.stream_and_get(down_id)

            return cluster_name

        finally:
            for temp_file_path in temp_files:
                try:
                    os.unlink(temp_file_path)
                except OSError:
                    pass

    os.environ['SKYPILOT_API_SERVER_ENDPOINT'] = api_server_endpoint

    cwd = os.getcwd()
    try:
        if yaml_file.startswith(('http://', 'https://')):
            resp = httpx.get(yaml_file, timeout=10)
            resp.raise_for_status()
            cfg = yaml.safe_load(resp.text)
            name_hint = os.path.splitext(os.path.basename(yaml_file))[0]
            return _launch_from_config(cfg, name_hint, credentials,
                                       envs_override)

        expanded = os.path.expanduser(yaml_file)
        if not os.path.exists(expanded):
            raise RuntimeError(f'YAML file {expanded} does not exist')

        workdir = os.path.dirname(expanded) or cwd
        os.chdir(workdir)
        with open(expanded, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        name_hint = os.path.splitext(os.path.basename(expanded))[0]
        return _launch_from_config(cfg, name_hint, credentials, envs_override)
    finally:
        os.chdir(cwd)
