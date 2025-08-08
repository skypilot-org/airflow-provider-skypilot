import os
import tempfile
from unittest.mock import MagicMock, mock_open, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Variable

from skypilot_provider.operators.cluster import (SkyPilotClusterOperator,
                                                 run_sky_task_with_credentials)

# [NOTE] These unit tests mock external dependencies and don't test:
# - Real SkyPilot API integration and cluster lifecycle
# - Actual cloud provider authentication and credential validation
# - PythonVirtualenvOperator with real package installation
# - File system operations and git repository access
# - YAML parsing with complex SkyPilot configurations
# - Network failures, timeouts, and resource constraints
# - End-to-end DAG execution and task dependencies
# Integration/system tests are needed to verify real-world functionality.


@pytest.fixture
def basic_operator_kwargs():
    return {
        'task_id': 'test_task',
        'base_path': '/test/path',
        'yaml_path': 'test.sky.yaml'
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
