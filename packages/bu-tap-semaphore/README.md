# BetterUp semaphore Tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data

This tap:

- Pulls raw job data from [Semaphore](https://github.com/semaphoreci/api/blob/master/semaphore/jobs.v1alpha.proto#L14)
- Outputs the schema for each resource
- Performs a full synchronization off the last three hours of job information on each execution.

# Installation

`poetry` is used to install the tap for both normal and development installations.

```
poetry install
```

To run the unit tests run the first command under the root directory or second for additional test coverage.
```
poetry run pytest tests/unit
poetry run pytest --cov=bu_tap_semaphore tests/unit 
```


# Usage semaphore-snowflake-sync

Before using the semaphore tap, you must create a `semaphore.json` and `snowflake.json` file.

To make things easier there is a command-line tool to generate configs for you.
```
poetry run create-semaphore-configs --token <semaphore api token>
```
After running you should see two files. (1) `semaphore.json` contains the API token necessary for `bu-tap-semaphore`. 
It should contain two entries called `'semaphore_token'` that maps to our API token from semaphore and 
`'semaphore_hours'` is the number of hours we want to sync. 

`semaphore.json` should look like:
```
{
    "semaphore_token": "XXXXXXXXXXXXXXXX",
    "semaphore_hours": "X"
}
```
(2) `snowflake.json` contains the database configuration required for the database connection information `target-snowflake`.
Similarly, we need the database connection information. The specific values can be retrieved from the Heroku details from the
[betterup-analytics dashboard](https://dashboard.heroku.com/apps/betterup-analytics). The following environment variables are
needed to execute the tap locally.

`snowflake.json` should look like:
```
{
    "account": os.getenv('SNOWFLAKE_ACCOUNT'),
    "user": os.getenv('SNOWFLAKE_USER'),
    "password": os.getenv('SNOWFLAKE_PASSWORD'),
    "dbname": os.getenv('SNOWFLAKE_DB'),
    "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
    "default_target_schema": os.getenv('SNOWFLAKE_SEMAPHORE_SCHEMA'),
    "stage": os.getenv('TAP_SEMAPHORE_STAGE'),
    "file_format": os.getenv('TAP_SEMAPHORE_FILE_FORMAT'),
    "aws_access_key_id": os.getenv('TAP_S3_ACCESS_KEY'),
    "aws_secret_access_key": os.getenv('TAP_S3_SECRET_KEY'),
    "s3_key_prefix": os.getenv('TAP_SEMAPHORE_S3_KEY_PREFIX'),
    "s3_bucket": os.getenv("TAP_S3_BUCKET"),
    "default_target_schema_select_permission": os.getenv("SNOWFLAKE_TRANSFORMER_ROLE"),
}
```

# Usage semaphore-s3-sync

As above, a `semaphore.json` file can be generated using the command below. Note: this will also generate a `snowflake.json` file but will not be used.
```
poetry run create-semaphore-configs --token <semaphore api token>
```
To run this job locally, you may need to define AWS access tokens. 
```
export AWS_ACCESS_KEY_ID="YOUR-ACCESS-KEY-HERE"
export AWS_SECRET_ACCESS_KEY="YOUR-SECRET-KEY-HERE"
```


## Discover available schemas

Each type of data we pull from semaphore conforms to a schema. To discover which schemas are available, run:

```
poetry run bu-tap-semaphore -c semaphore.json --discover
```

## Synchronize the data from semaphore to our Snowflake database.

Handing off data from `bu-tap-semaphore` is pretty straight-forward. `bu-tap-semaphore` emits records
across stdout using the Singer spec. This means we can pipe the output to the `target-snowflake` or `target-s3` Singer
target. First, we'll have to create another configuration file to establish the database configuration.

If you ran `create-semaphore-configs` above you are good to go and can proceed.

semaphore-snowflake-sync:
```
poetry run bu-tap-semaphore -c semaphore.json | poetry run target-snowflake -c snowflake.json
```

semaphore-s3-sync: 
```
  poetry run bu-tap-semaphore --config semaphore.json | (cd ../target-s3 && poetry run target-s3 -b "${SEMAPHORE_AWS_BUCKET}")
```