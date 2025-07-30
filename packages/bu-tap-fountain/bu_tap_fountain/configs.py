"""
Creates the required configuration files for the
fountain tap and snowflake target.

Requires that the TAP_FOUNTAIN_URL exists
as an environment variable or specify the database
URI via the --uri option. It should specify the
URI for an account to write to the Postgres database.

Likewise, FOUNTAIN_API_TOKEN must exist as an
environment variable or provided by the --token option.
That token value can be retrieved from the Fountain admin
panel and stored as configuration variable in Heroku.
"""
import json
import os

import click


@click.command()
@click.option("--token", default=None, help="Fountain API token value.")
def create_configs(token):

    snowflake_config = create_snowflake_config()
    with open("snowflake.json", "w") as fh:
        fh.write(json.dumps(snowflake_config))

    # Create the fountain.json configuration file for bu-tap-fountain
    fountain_config = create_fountain_config(token)

    with open("fountain.json", "w") as fh:
        fh.write(json.dumps(fountain_config))


def create_fountain_config(token=None):
    """Create a dictionary containing the bu-tap-fountain configuration

    If token is None, then try to retrieve the value from FOUNTAIN_API_TOKEN
    environment variable. If that does not exist then a KeyError is raised.
    """
    if token is None:
        token = os.getenv("FOUNTAIN_API_TOKEN")

    if token is None:
        raise KeyError(
            "FOUNTAIN_API_TOKEN is missing from the environment variables. "
            "Please use the --token option or set FOUNTAIN_API_TOKEN."
        )
    config = {"fountain_api_token": token}

    return config


def create_snowflake_config():
    config = {
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_USER'),
        "password": os.getenv('SNOWFLAKE_PASSWORD'),
        "dbname": os.getenv('SNOWFLAKE_DB'),
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "default_target_schema": os.getenv('SNOWFLAKE_FOUNTAIN_SCHEMA'),
        "stage": os.getenv('TAP_FOUNTAIN_STAGE'),
        "file_format": os.getenv('TAP_FOUNTAIN_FILE_FORMAT'),
        "aws_access_key_id": os.getenv('TAP_S3_ACCESS_KEY'),
        "aws_secret_access_key": os.getenv('TAP_S3_SECRET_KEY'),
        "s3_key_prefix": os.getenv('TAP_FOUNTAIN_S3_KEY_PREFIX'),
        "s3_bucket": os.getenv("TAP_S3_BUCKET"),
        "default_target_schema_select_permission": os.getenv("SNOWFLAKE_TRANSFORMER_ROLE"),
    }
    return config


if __name__ == "__main__":
    create_configs()  # pylint: disable=no-value-for-parameter
