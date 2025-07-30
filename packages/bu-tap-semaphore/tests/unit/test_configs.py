import filecmp
import json
import mock
import os

from bu_tap_semaphore import configs
from click.testing import CliRunner
from .fixtures import fixtures


def test_create_configs():
    mock_env_vars = fixtures.get_mock_env_vars()

    def read_json_file(path):
        with open(path) as f:
            return json.load(f)

    with mock.patch.dict(os.environ, mock_env_vars):
        runner = CliRunner()
        runner.invoke(configs.create_configs)

        test_semaphore_json = read_json_file("semaphore.json")
        expected_semaphore_json = read_json_file(
            "tests/unit/fixtures/expected_semaphore.json"
        )

        assert test_semaphore_json == expected_semaphore_json

        test_snowflake_json = read_json_file("snowflake.json")
        expected_snowflake_json = read_json_file(
            "tests/unit/fixtures/expected_snowflake.json"
        )

        assert test_snowflake_json == expected_snowflake_json

    os.remove("semaphore.json")
    os.remove("snowflake.json")
