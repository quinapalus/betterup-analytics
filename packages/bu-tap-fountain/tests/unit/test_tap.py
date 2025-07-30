import filecmp
import json
import os
import sys

from bu_tap_fountain import tap
from .fixtures import fixtures

def test_write_applicants(requests_mock):
    expected_applicants_response = fixtures.get_list_applicants()
    requests_mock.get(tap.APPLICANTS_URL, json=expected_applicants_response)
    config = fixtures.get_fountain_config()

    test_output = "tests/unit/test_write_applicants_output"
    expected_output = "tests/unit/fixtures/expected_write_applicants_output"
    with open(test_output, "w") as sys.stdout:
        applicant_ids = tap.write_applicants(config)
        for i, applicant in enumerate(expected_applicants_response["applicants"]):
            assert applicant["id"] == applicant_ids[i]
        assert filecmp.cmp(test_output, expected_output)
    sys.stdout = sys.__stdout__
    os.remove(test_output)


def test_write_transitions(requests_mock):
    applicant_ids = fixtures.get_applicant_ids()
    config = fixtures.get_fountain_config()

    url1 = tap.TRANSITIONS_URL.format(id=applicant_ids[0])
    requests_mock.get(url1, json=fixtures.get_list_transitions_1())

    url2 = tap.TRANSITIONS_URL.format(id=applicant_ids[1])
    requests_mock.get(url2, json=fixtures.get_list_transitions_2())

    test_output = "tests/unit/test_write_transitions_output"
    expected_output = "tests/unit/fixtures/expected_write_transitions_output"
    with open(test_output, "w") as sys.stdout:
        tap.write_transitions(config, applicant_ids)

        assert filecmp.cmp(test_output, expected_output)

    sys.stdout = sys.__stdout__
    os.remove(test_output)

def test_write_funnels(requests_mock):
    expected_funnels_response = fixtures.get_list_funnels()
    requests_mock.get(tap.OPENINGS_URL, json=expected_funnels_response)
    config = fixtures.get_fountain_config()
    test_output = "tests/unit/test_write_funnels_output"
    with open(test_output, "w") as sys.stdout:
        funnel_ids = tap.write_funnels(config)
        assert expected_funnels_response["funnels"][0]["id"] in funnel_ids
    sys.stdout = sys.__stdout__
    os.remove(test_output)

def test_discover():
    catalog = tap.discover()
    expected_discover_output = fixtures.get_discover()
    for index, entry in enumerate(catalog["streams"]):
        expected_discover_entry = expected_discover_output["streams"][index]
        assert entry["stream"] == expected_discover_entry["stream"]
        assert entry["tap_stream_id"] == expected_discover_entry["tap_stream_id"]
        assert entry["metadata"] == expected_discover_entry["metadata"]
        assert entry["key_properties"] == expected_discover_entry["key_properties"]
        for key, value in entry["schema"]["properties"].items():
            assert value == expected_discover_entry["schema"]["properties"][key]
