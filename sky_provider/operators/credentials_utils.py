"""Cloud credentials handlers for SkyPilot provider."""

import abc
import json
from typing import Dict, TypedDict

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CredentialsOverride(TypedDict):
    aws: str | None
    gcp: str | None


class ClusterCredentials(TypedDict):
    # Map of file paths to mount on the cluster and their contents
    file_mounts: Dict[str, str]
    # Map of environment variable names to set on the cluster and their values.
    env_vars: Dict[str, str]


class CloudCredentialsHandler(abc.ABC):
    """Abstract base class for handling cloud provider credentials."""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @abc.abstractmethod
    def get_cluster_credentials(self) -> ClusterCredentials:
        """
        Get the credentials for this cloud provider to mount on the cluster.

        Returns:
            ClusterCredentials containing file_mounts and env_vars.

        Raises:
            AirflowNotFoundException: If the connection is not found.
            AirflowException: If the connection is found but no credentials are available.
        """
        raise NotImplementedError


class AwsCredentialsHandler(CloudCredentialsHandler):
    """Handler for AWS credentials using AwsBaseHook."""

    def get_cluster_credentials(self) -> ClusterCredentials:
        hook = AwsBaseHook(aws_conn_id=self.conn_id)
        status, message = hook.test_connection()
        if not status:
            raise AirflowException(f'AWS connection check failed: {message}')

        creds = hook.get_credentials()
        aws_creds_content = f'''[default]
aws_access_key_id = {creds.access_key}
aws_secret_access_key = {creds.secret_key}
'''
        if creds.token:
            aws_creds_content += f'aws_session_token = {creds.token}\n'

        file_mounts = {'/tmp/aws-credentials': aws_creds_content}
        env_vars = {'AWS_SHARED_CREDENTIALS_FILE': '/tmp/aws-credentials'}
        return {'file_mounts': file_mounts, 'env_vars': env_vars}


class GcpCredentialsHandler(CloudCredentialsHandler):
    """Handler for GCP credentials using GoogleBaseHook."""

    def get_cluster_credentials(self) -> ClusterCredentials:
        hook = GoogleBaseHook(gcp_conn_id=self.conn_id)
        status, message = hook.test_connection()
        if not status:
            raise AirflowException(f'GCP connection check failed: {message}')

        test = hook.get_credentials()
        print(f'test.get_cred_info(): {test.get_cred_info()}')
        conn = hook.get_connection(hook.gcp_conn_id)
        service_account_json = conn.extra_dejson.get('keyfile_dict')

        if not service_account_json:
            raise AirflowException(
                'No service account key found in GCP connection. '
                'Please add it to keyfile_dict in the connection.')

        if isinstance(service_account_json, str):
            try:
                service_account_json = json.loads(service_account_json)
            except json.JSONDecodeError as exc:
                raise AirflowException(
                    f'Invalid JSON format in keyfile_dict: {service_account_json}'
                ) from exc

        file_mounts = {
            '/tmp/gcp-service-account.json':
            json.dumps(service_account_json, indent=2)
        }
        env_vars = {
            'GOOGLE_APPLICATION_CREDENTIALS': '/tmp/gcp-service-account.json'
        }
        # if service_account_json.get('project_id'):
        #     env_vars['GOOGLE_CLOUD_PROJECT'] = service_account_json['project_id']

        return {'file_mounts': file_mounts, 'env_vars': env_vars}


CREDENTIAL_HANDLERS = {
    'aws': AwsCredentialsHandler,
    'gcp': GcpCredentialsHandler,
}
