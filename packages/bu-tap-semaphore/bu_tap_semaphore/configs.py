"""
Creates the required configuration files for the
semaphore tap and snowflake target.

Likewise, SEMAPHORE_API_TOKEN must exist as an
environment variable or provided by the --token option.
That token value can be retrieved from the semaphore admin
panel and stored as configuration variable in Heroku.
"""
import json
import os

import click


@click.command()
@click.option("--token", default=None, help="semaphore API token value.")
def create_configs(token):
    snowflake_config = create_snowflake_config()
    with open("snowflake.json", "w") as fh:
        fh.write(json.dumps(snowflake_config))

    # Create the semaphore.json configuration file for bu-tap-semaphore
    semaphore_config = create_semaphoreci_config(token)

    with open("semaphore.json", "w") as fh:
        fh.write(json.dumps(semaphore_config))


def create_semaphoreci_config(token=None):
    """Create a dictionary containing the bu-tap-semaphoreci configuration

    If token is None, then try to retrieve the value from SEMAPHORE_API_TOKEN
    environment variable. If that does not exist then a KeyError is raised.
    """
    if token is None:
        token = os.getenv("SEMAPHORE_TOKEN")

    if token is None:
        raise KeyError(
            "SEMAPHORE_TOKEN is missing from the environment variables. "
            "Please use the --token option or set SEMAPHORE_TOKEN."
        )
    config = {
        "semaphore_token": token,
        "semaphore_hours": os.getenv("SEMAPHORE_HOURS"),
    }

    return config


def create_snowflake_config():
    config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "dbname": os.getenv("SNOWFLAKE_DB"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "default_target_schema": os.getenv("SNOWFLAKE_SEMAPHORE_SCHEMA"),
        "stage": os.getenv("TAP_SEMAPHORE_STAGE"),
        "file_format": os.getenv("TAP_SEMAPHORE_FILE_FORMAT"),
        "aws_access_key_id": os.getenv("TAP_S3_ACCESS_KEY"),
        "aws_secret_access_key": os.getenv("TAP_S3_SECRET_KEY"),
        "s3_key_prefix": os.getenv("TAP_SEMAPHORE_S3_KEY_PREFIX"),
        "s3_bucket": os.getenv("TAP_S3_BUCKET"),
        "default_target_schema_select_permission": os.getenv(
            "SNOWFLAKE_TRANSFORMER_ROLE"
        ),
    }
    return config


if __name__ == "__main__":
    create_configs()  # pylint: disable=no-value-for-parameter
