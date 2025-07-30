# BetterUp Zendesk to S3 

This tap:

- Pulls raw data from [Zeddesk](https://www.betterup.zendesk.com) using [tap-zendesk](https://github.com/singer-io/tap-zendesk)
- Sends extracted data to S3 using [target-s3](https://github.com/betterup/betterup-analytics/tree/main/packages/target-s3).

# Installation

`poetry` is used to install the tap for both normal and development installations.

```
poetry install
```

To run the unit tests run the first command under the root directory or second for additional test coverage.
```
poetry run pytest tests/unit
poetry run pytest --cov=bu_zendesk_to_s3 tests/unit 
```

# Usage

Before using the tap, you must create a `zendesk.json` .
To make things easier there is a command-line tool to generate configs for you.
```
poetry run create-zendesk-configs
```
After running you should see a `zendesk.json` that looks like:
```
{
    "access_token": "XXXXXX",
    "subdomain": "XXXXXX",
    "start_date": "XXXXXX"
}
```

## Synchronize the data from Zendesk to our S3 bucket.

If you ran `create-zendesk-configs` above you are good to go and can proceed.

```
poetry run tap-zendesk --config zendesk.json --catalog catalog.json | (cd ../target-s3 && poetry run target-s3 -b "${ZENDESK_AWS_BUCKET}")
```
Note: if this does not work locally, you may need to add your AWS secrets as environment variables. To do this, run the following commands in the terminal: 
```
export AWS_ACCESS_KEY_ID="XXXXXX"
export AWS_SECRET_ACCESS_KEY="XXXXXX"
```
