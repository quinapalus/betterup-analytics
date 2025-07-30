import json
import mock
import os
from bu_tap_qualtrics import configs

def test_create_qualtrics_config():
    mock_env_vars = {
        "QUALTRICS_CLIENT_ID": "test",
        "QUALTRICS_CLIENT_SECRET": "test"
    }
    
    with mock.patch.dict(os.environ, mock_env_vars):
        configs.create_qualtrics_configs()
        test_output = "./qualtrics.json"
        expected_output = "tests/unit/fixtures/expected_qualtrics.json"

        test_f = open(test_output)
        expected_f = open(expected_output)

        test_json = json.load(test_f)
        expected_json = json.load(expected_f)

        del test_json["start_date"]
        del expected_json["start_date"]

        assert(test_json == expected_json)

        test_f.close()
        expected_f.close()

        os.remove(test_output)
