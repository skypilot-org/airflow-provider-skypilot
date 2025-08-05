from typing import Dict, Sequence, Type, TypedDict

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import \
    PythonVirtualenvOperator
from airflow.sdk import Variable

from .credentials_utils import (CREDENTIAL_HANDLERS, CloudCredentialsHandler,
                                ClusterCredentials, CredentialsOverride)


class SkyTaskOperator(PythonVirtualenvOperator):
    """
    Creates a SkyPilot cluster and runs a task,
    as defined in the task YAML file.

    Args:
        base_path: Base path (local directory or git repo URL)
        yaml_path: Path to the YAML file (relative to base_path)
        git_branch: Optional git branch to checkout when base_path is a git repository.
        credentials_override: Credentials override configuration.
        envs_override: Additional environment variables to override.
        skypilot_version: Optional version of SkyPilot to use.
    """

    template_fields: Sequence[str] = (
        'base_path',
        'yaml_path',
        'git_branch',
        'credentials_override',
        'envs_override',
        'skypilot_version',
    )

    def __init__(
        self,
        *,
        base_path: str,
        yaml_path: str,
        git_branch: str | None = None,
        credentials_override: CredentialsOverride | None = None,
        envs_override: Dict[str, str] | None = None,
        skypilot_version: str | None = None,
        **kwargs,
    ) -> None:
        if not yaml_path or not yaml_path.strip():
            raise ValueError('yaml_path must be a non-empty string')

        skypilot_requirement = ('skypilot[all]' if skypilot_version is None
                                else f'skypilot[all]=={skypilot_version}')
        super().__init__(
            python_callable=run_sky_task_with_credentials,
            requirements=[skypilot_requirement],
            python_version='3.11',
            # PythonVirtualenvOperator now uses uv by default if available,
            # so we need to use --pre, as the Azure CLI has an issue with uv.
            pip_install_options=['--pre'],
            **kwargs,
        )

        self.base_path = base_path
        self.yaml_path = yaml_path
        self.git_branch = git_branch
        self.credentials_override = credentials_override
        self.envs_override = envs_override or {}
        self.skypilot_version = skypilot_version

    def execute(self, context):
        # Get credentials in the main Airflow environment,
        # as it depends on Airflow providers not
        # available later in the virtualenv.
        credentials = self._get_credentials()

        api_server_endpoint = Variable.get('SKYPILOT_API_SERVER_ENDPOINT')
        if not api_server_endpoint:
            raise AirflowException(
                'SKYPILOT_API_SERVER_ENDPOINT Variable is not set')

        self.op_args = [
            self.base_path,
            self.yaml_path,
            self.git_branch,
            credentials,
            self.envs_override,
            api_server_endpoint,
        ]
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
                handler: CloudCredentialsHandler = handler_class(str(conn_id))
                cluster_credentials = handler.get_cluster_credentials()
                file_mounts.update(cluster_credentials['file_mounts'])
                env_vars.update(cluster_credentials['env_vars'])

        return {'file_mounts': file_mounts, 'env_vars': env_vars}


def run_sky_task_with_credentials(
    base_path: str,
    yaml_path: str,
    git_branch: str | None,
    credentials: ClusterCredentials,
    envs_override: Dict[str, str],
    api_server_endpoint: str,
):
    import os
    import subprocess
    import tempfile
    import uuid

    import yaml

    def _run_sky_task_with_credentials(yaml_path: str,
                                       credentials: ClusterCredentials,
                                       envs_override: Dict[str, str]):
        """Internal helper to run the sky task with credential artifacts."""
        import sky

        with open(os.path.expanduser(yaml_path), 'r',
                  encoding='utf-8') as file:
            task_config = yaml.safe_load(file)

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
            cluster_uid = str(uuid.uuid4())[:4]
            task_name = os.path.splitext(os.path.basename(yaml_path))[0]
            cluster_name = f'{task_name}-{cluster_uid}'

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
        if base_path.startswith(('http://', 'https://', 'git://')):
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    # Clone the repository
                    subprocess.run(
                        ['git', 'clone', base_path, temp_dir],
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                except subprocess.CalledProcessError as exc:
                    raise AirflowException(
                        f'Failed to clone repository {base_path}: {exc.stderr}'
                    ) from exc

                # Checkout specific branch if provided
                if git_branch:
                    try:
                        subprocess.run(
                            ['git', 'checkout', git_branch],
                            cwd=temp_dir,
                            capture_output=True,
                            text=True,
                            check=True,
                        )
                    except subprocess.CalledProcessError as exc:
                        raise AirflowException(
                            f'Failed to checkout branch {git_branch}: {exc.stderr}'
                        ) from exc

                full_yaml_path = os.path.join(temp_dir, yaml_path)
                if not os.path.exists(full_yaml_path):
                    raise AirflowException(
                        f'YAML file {yaml_path} not found in repository {base_path}'
                    )

                os.chdir(temp_dir)
                return _run_sky_task_with_credentials(full_yaml_path,
                                                      credentials,
                                                      envs_override)
        else:
            full_yaml_path = os.path.join(base_path, yaml_path)
            os.chdir(base_path)

            if not os.path.exists(full_yaml_path):
                raise AirflowException(
                    f'YAML file {full_yaml_path} does not exist')

            # Run the sky task
            return _run_sky_task_with_credentials(full_yaml_path, credentials,
                                                  envs_override)
    finally:
        os.chdir(cwd)
