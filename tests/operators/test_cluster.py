import os
import tempfile
from unittest.mock import MagicMock, mock_open, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Variable

from skypilot_provider.operators.cluster import SkyPilotClusterOperator


@pytest.fixture
def basic_operator_kwargs():
    return {
        'task_id': 'test_task',
        'base_path': '/test/path',
        'yaml_path': 'test.sky.yaml'
    }


@pytest.fixture
def mock_credentials():
    return {
        'file_mounts': {
            '/tmp/gcp-service-account.json': '{"type": "service_account"}',
        },
        'env_vars': {
            'GOOGLE_APPLICATION_CREDENTIALS': '/tmp/gcp-service-account.json'
        }
    }


class TestSkyPilotClusterOperator:

    def test_init_with_valid_params(self, basic_operator_kwargs):
        operator = SkyPilotClusterOperator(**basic_operator_kwargs)
        assert operator.base_path == '/test/path'
        assert operator.yaml_path == 'test.sky.yaml'
        assert operator.git_branch is None
        assert operator.credentials_override is None
        assert operator.envs_override == {}
        assert operator.skypilot_version is None

    @pytest.mark.parametrize("yaml_path", ["", "   "])
    def test_init_with_invalid_yaml_path(self, basic_operator_kwargs,
                                         yaml_path):
        operator_kwargs = basic_operator_kwargs.copy()
        operator_kwargs['yaml_path'] = yaml_path
        with pytest.raises(ValueError,
                           match='yaml_path must be a non-empty string'):
            SkyPilotClusterOperator(**operator_kwargs)

    def test_init_with_all_params(self, basic_operator_kwargs):
        operator_kwargs = basic_operator_kwargs.copy()
        credentials_override = {'aws': 'aws_conn', 'gcp': 'gcp_conn'}
        envs_override = {'TEST_VAR': 'test_value'}
        operator_kwargs['credentials_override'] = credentials_override
        operator_kwargs['envs_override'] = envs_override
        operator_kwargs['skypilot_version'] = '0.10.0'
        operator_kwargs['git_branch'] = 'main'

        operator = SkyPilotClusterOperator(**operator_kwargs)

        assert operator.git_branch == 'main'
        assert operator.credentials_override == credentials_override
        assert operator.envs_override == envs_override
        assert operator.skypilot_version == '0.10.0'

    @pytest.mark.parametrize("skypilot_version,expected_requirement", [
        (None, 'skypilot[all]'),
        ('0.10.0', 'skypilot[all]==0.10.0'),
        ('1.0.0.dev20250808', 'skypilot-nightly[all]==1.0.0.dev20250808'),
    ])
    @patch(
        'skypilot_provider.operators.cluster.PythonVirtualenvOperator.__init__'
    )
    def test_skypilot_requirements_versions(self, mock_init,
                                            basic_operator_kwargs,
                                            skypilot_version,
                                            expected_requirement):
        mock_init.return_value = None
        operator_kwargs = basic_operator_kwargs.copy()
        if skypilot_version is not None:
            operator_kwargs['skypilot_version'] = skypilot_version

        SkyPilotClusterOperator(**operator_kwargs)
        mock_init.assert_called_once()
        kwargs = mock_init.call_args[1]
        assert expected_requirement in kwargs['requirements']

    @patch('skypilot_provider.operators.cluster.Variable.get')
    def test_execute_missing_api_server_endpoint(self, mock_variable_get,
                                                 basic_operator_kwargs):
        mock_variable_get.return_value = None
        operator = SkyPilotClusterOperator(**basic_operator_kwargs)

        with pytest.raises(
                AirflowException,
                match='SKYPILOT_API_SERVER_ENDPOINT Variable is not set'):
            operator.execute({})

    @patch('skypilot_provider.operators.cluster.Variable.get')
    @patch(
        'skypilot_provider.operators.cluster.PythonVirtualenvOperator.execute')
    def test_execute_success(self, mock_super_execute, mock_variable_get,
                             basic_operator_kwargs, mock_credentials):
        mock_variable_get.return_value = 'http://test-api-server'

        operator = SkyPilotClusterOperator(**basic_operator_kwargs)
        with patch.object(operator,
                          '_get_credentials',
                          return_value=mock_credentials):
            operator.execute({})
        mock_super_execute.assert_called_once()

        expected_op_args = [
            '/test/path', 'test.sky.yaml', None, mock_credentials, {},
            'http://test-api-server'
        ]
        assert operator.op_args == expected_op_args

    def test_get_credentials_no_override(self, basic_operator_kwargs):
        operator = SkyPilotClusterOperator(**basic_operator_kwargs)
        credentials = operator._get_credentials()
        assert credentials == {'file_mounts': {}, 'env_vars': {}}

    @patch('skypilot_provider.operators.cluster.CREDENTIAL_HANDLERS')
    def test_get_credentials_with_override(self, mock_handlers,
                                           basic_operator_kwargs):
        mock_aws_handler = MagicMock()
        mock_aws_handler.get_cluster_credentials.return_value = {
            'file_mounts': {
                '/tmp/aws-creds': 'aws-content'
            },
            'env_vars': {
                'AWS_ACCESS_KEY_ID': 'aws-key'
            }
        }

        mock_gcp_handler = MagicMock()
        mock_gcp_handler.get_cluster_credentials.return_value = {
            'file_mounts': {
                '/tmp/gcp-service-account.json': 'gcp-content'
            },
            'env_vars': {
                'GOOGLE_APPLICATION_CREDENTIALS':
                '/tmp/gcp-service-account.json'
            }
        }

        mock_handlers.items.return_value = [
            ('aws', lambda x: mock_aws_handler),
            ('gcp', lambda x: mock_gcp_handler)
        ]

        credentials_override = {'aws': 'aws_conn', 'gcp': 'gcp_conn'}
        basic_operator_kwargs['credentials_override'] = credentials_override

        operator = SkyPilotClusterOperator(**basic_operator_kwargs)
        credentials = operator._get_credentials()

        expected = {
            'file_mounts': {
                '/tmp/aws-creds': 'aws-content',
                '/tmp/gcp-service-account.json': 'gcp-content'
            },
            'env_vars': {
                'AWS_ACCESS_KEY_ID': 'aws-key',
                'GOOGLE_APPLICATION_CREDENTIALS':
                '/tmp/gcp-service-account.json'
            }
        }
        assert credentials == expected
