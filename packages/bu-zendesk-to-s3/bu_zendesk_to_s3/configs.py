#!python3

import datetime
import json
import os


def create_zendesk_configs():
    zendesk_configs = {
        "access_token": os.getenv("ZENDESK_ACCESS_TOKEN"),
        "subdomain": "betterup",
        "start_date": (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }
    with open("zendesk.json", "w") as f:
        f.write(json.dumps(zendesk_configs))


if __name__ == "__main__":
    create_zendesk_configs()  # pylint: disable=no-value-for-parameter # pragma: no cover
