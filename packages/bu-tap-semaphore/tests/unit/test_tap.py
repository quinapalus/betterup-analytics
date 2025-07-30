import arrow
import filecmp
import json
import os
import sys

from bu_tap_semaphore import tap
from .fixtures import fixtures


def test_sync(requests_mock, mocker):
    requests_mock.get(tap.REQUEST_URL, json=fixtures.get_semaphore_response())
    mocker.patch("arrow.utcnow", return_value=arrow.get(int(fixtures.get_time())))
    config = fixtures.get_semaphore_config()

    test_output = "tests/unit/test_sync_out"
    expected_output = "tests/unit/fixtures/expected_singer_output"
    with open(test_output, "w") as sys.stdout:
        tap.sync(config, None)
        assert filecmp.cmp(test_output, expected_output)

    sys.stdout = sys.__stdout__
    os.remove(test_output)


def test_discover():
    catalog = tap.discover()
    expected_discover_output = fixtures.get_discover()

    assert catalog == expected_discover_output
