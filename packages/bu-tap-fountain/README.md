# BetterUp Fountain Tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Fountain](https://www.fountain.com/betterup)
- Extracts the following resources:
  - [Applicants](https://developer.fountain.com/docs/get-apiv2applicants-list-applicants)
  - [Transitions](https://developer.fountain.com/docs/get-transition-history)
- Outputs the schema for each resource
- Performs a full synchronization on each execution.

# Installation

`poetry` is used to install the tap for both normal and development installations.

```
poetry install
```

To run the unit tests run the first command under the root directory or second for additional test coverage.
```
poetry run pytest tests/unit
poetry run pytest --cov=bu_tap_fountain tests/unit 
```

# Usage

Before using the tap, you must create a `fountain.json` and `snowflake.json` file.
To make things easier there is a command-line tool to generate configs for you.
```
poetry run create-configs
```

After running you should see two files: (1) `fountain.json` and (2) `snowflake.json`. 

The `fountain.json`  contains the API token necessary for `bu-tap-fountain` file. It needs to contain a single entry called
`'fountain_api_token'` that maps to our API token from Fountain. You can retrieve that value by going
to the [Fountain developer page](https://www.fountain.com/betterup/account/api) and clicking the "Show API keys" button.

`fountain.json` should look like:
```
{
    'fountain_api_token': 'XXXXXXXXXXXXXXXX'
}
```

`snowflake.json` contains the database configuration required for `target-snowflake`
`snowflake.json` should look like: 
```
{
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
```

## Discover available schemas

Each type of data we pull from Fountain conforms to a schema. To discover which schemas are available, run:

```
bu-tap-fountain -c config.json --discover
```

## Synchronize the data from Fountain to our Snowflake database.

Handing off data from `bu-tap-fountain` is pretty straight-forward. `bu-tap-fountain` emits records
across stdout using the Singer spec. This means we can pipe the output to the `pipelinewise-target-snowflake` Singer
target. First, we'll have to create another configuration file to establish the database configuration.

If you ran `create-configs` above you are good to go and can proceed.

```
poetry run bu-tap-fountain -c fountain.json | poetry run target-snowflake -c snowflake.json
```
