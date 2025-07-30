import json
import os
from bu_tap_looker import configs

def test_create_looker_config():
    configs.create_looker_configs()
    test_output = "./looker.json"

    with open(test_output) as test_f:
        test_json = json.load(test_f)

        test_f.close()

        assert("start_date" in test_json)
    os.remove(test_output)
