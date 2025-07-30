import datetime
import json
import mock
import os

from bu_zendesk_to_s3 import configs
from .fixtures import fixtures


def test_create_configs(mocker):
    mock_env_vars = fixtures.get_mock_env_vars()

    def read_json_file(path):
        with open(path) as f:
            return json.load(f)

    with mock.patch.dict(os.environ, mock_env_vars):
        configs.create_zendesk_configs()

        test_zendesk_json = read_json_file("zendesk.json")
        expected_zendesk_json = read_json_file("tests/unit/fixtures/expected_zendesk.json")

        assert (test_zendesk_json["access_token"] == expected_zendesk_json["access_token"])
        assert test_zendesk_json["subdomain"] == expected_zendesk_json["subdomain"]
    
    os.remove("zendesk.json")
