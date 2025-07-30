import json
import os
import pytest

from bu_tap_fountain import configs
from click.testing import CliRunner


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("FOUNTAIN_API_TOKEN", "FOUNTAIN_API_TOKEN")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_ACCOUNT")
    monkeypatch.setenv("SNOWFLAKE_USER", "SNOWFLAKE_USER")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "SNOWFLAKE_PASSWORD")
    monkeypatch.setenv("SNOWFLAKE_DB", "SNOWFLAKE_DB")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_WAREHOUSE")
    monkeypatch.setenv("SNOWFLAKE_FOUNTAIN_SCHEMA", "SNOWFLAKE_FOUNTAIN_SCHEMA")
    monkeypatch.setenv("TAP_FOUNTAIN_STAGE", "TAP_FOUNTAIN_STAGE")
    monkeypatch.setenv("TAP_FOUNTAIN_FILE_FORMAT", "TAP_FOUNTAIN_FILE_FORMAT")
    monkeypatch.setenv("TAP_S3_ACCESS_KEY", "TAP_S3_ACCESS_KEY")
    monkeypatch.setenv("TAP_S3_SECRET_KEY", "TAP_S3_SECRET_KEY")
    monkeypatch.setenv("TAP_FOUNTAIN_S3_KEY_PREFIX", "TAP_FOUNTAIN_S3_KEY_PREFIX")
    monkeypatch.setenv("TAP_S3_BUCKET", "TAP_S3_BUCKET")
    monkeypatch.setenv("SNOWFLAKE_TRANSFORMER_ROLE", "SNOWFLAKE_TRANSFORMER_ROLE")


def test_create_fountain_config_default(mock_env):
    test_fountain_output = "fountain.json"
    expected_fountain_output = "tests/unit/fixtures/expected_fountain.json"
    test_snowflake_output = "snowflake.json"
    expected_snowflake_output = "tests/unit/fixtures/expected_snowflake.json"

    runner = CliRunner()
    runner.invoke(configs.create_configs)

    test_fountain_f = open(test_fountain_output)
    expected_fountain_f = open(expected_fountain_output)
    test_fountain_json = json.load(test_fountain_f)
    expected_fountain_json = json.load(expected_fountain_f)

    test_snowflake_f = open(test_snowflake_output)
    expected_snowflake_f = open(expected_snowflake_output)
    test_snowflake_json = json.load(test_snowflake_f)
    expected_snowflake_json = json.load(expected_snowflake_f)

    assert test_fountain_json == expected_fountain_json
    assert test_snowflake_json == expected_snowflake_json

    test_fountain_f.close()
    expected_fountain_f.close()
    test_snowflake_f.close()
    expected_snowflake_f.close()

    os.remove(test_fountain_output)
    os.remove(test_snowflake_output)


def test_create_fountain_config_override(monkeypatch):
    runner = CliRunner()
    runner.invoke(configs.create_configs, ["--token", "oops"])

    test_fountain_output = "fountain.json"
    test_snowflake_output = "snowflake.json"
    test_fountain_f = open(test_fountain_output)
    test_fountain_json = json.load(test_fountain_f)

    assert test_fountain_json["fountain_api_token"] == "oops"

    os.remove(test_fountain_output)
    os.remove(test_snowflake_output)


def test_create_fountain_config_no_params(monkeypatch):
    monkeypatch.delenv("FOUNTAIN_API_TOKEN", raising=False)
    with pytest.raises(KeyError):
        config = configs.create_fountain_config()
