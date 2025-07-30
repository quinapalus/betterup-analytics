import json
import mock
import os
from bu_tap_twilio import configs

def test_create_twilio_configs():
    mock_env_vars = {
        "TWILIO_ACCOUNT_SID": "TEST",
        "TWILIO_AUTH_TOKEN": "TEST",
    }
    with mock.patch.dict(os.environ, mock_env_vars):
        configs.create_twilio_configs()
        test_output = "./twilio.json"
        expected_output = "tests/unit/fixtures/expected_twilio.json"

        test_f = open(test_output)
        expected_f = open(expected_output)

        test_json = json.load(test_f)
        expected_json = json.load(expected_f)

        # not comparing the start date because these are datetime objects dependent on current time
        del test_json["start_date"]
        del expected_json["start_date"]

        assert(test_json == expected_json)

        test_f.close()
        expected_f.close()

        os.remove(test_output)
