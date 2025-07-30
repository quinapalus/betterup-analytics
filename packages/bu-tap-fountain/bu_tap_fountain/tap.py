"""
Singer tap for pulling data from Fountain.

Currently supports:
    * Pulling applicant information
    * Pulling applicant stage transition history
    * Pull funnel (opening) information
"""
import json
import time

import singer
from singer import utils, Transformer
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from . import schema


REQUIRED_CONFIG_KEYS = ["fountain_api_token"]
LOGGER = singer.get_logger()

FOUNTAIN_API_URL = "https://api.fountain.com/v2"
APPLICANTS_URL = FOUNTAIN_API_URL + "/applicants"
TRANSITIONS_URL = APPLICANTS_URL + "/{id}/transitions"
OPENINGS_URL = FOUNTAIN_API_URL + "/funnels"


MAX_PAGES = 10000
"""Max number of pages to retrieve (prevents an infinite loop)"""

MAX_ATTEMPTS = 3
"""Max number of times to sleep before retrying after Too Many Requests error """


@utils.handle_top_exception(LOGGER)
def main():  # pragma: no cover
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        sync(args.config, args.state)


def discover():
    raw_schemas = {"applicants": schema.applicants, "transitions": schema.transitions, "funnels": schema.funnels}
    streams = []

    for schema_name, schema_details in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema_details,
            "metadata": {"selected": True},
            "key_properties": [],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def create_requests_session(headers):
    session = requests.Session()
    retry = Retry(total=3, read=3, connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(headers)
    return session


def sync(config, state):
    LOGGER.info("Writing applicants")

    write_funnels(config)
    applicant_ids = write_applicants(config)
    write_transitions(config, applicant_ids)


def write_applicants(config):
    applicant_ids = []

    api_key = config["fountain_api_token"]
    singer.write_schema("applicant", schema.applicants, ["id"])

    for applicant in list_applicants(api_key):
        applicant_ids.append(applicant["id"])

        applicant["funnel"] = applicant["funnel"]["title"]
        applicant["stage"] = applicant["stage"]["title"]

        applicant.update(applicant["data"])

        with Transformer() as transformer:
            record = transformer.transform(applicant, schema.applicants)

        singer.write_record("applicant", record)

    return applicant_ids


def list_applicants(api_key):
    current = 1
    last = None
    i = 0

    session = create_requests_session({"X-ACCESS-TOKEN": api_key})
    while current != None and current != last and i <= MAX_PAGES:
        response = session.get(APPLICANTS_URL, params={"page": current})

        response.raise_for_status()
        data = response.json()

        current = data["pagination"]["next"]
        last = data["pagination"]["last"]
        for applicant in data["applicants"]:
            yield applicant


def write_transitions(config, applicant_ids):
    api_key = config["fountain_api_token"]
    # stage_id can be null, so we use stage_name as part of the primary key
    singer.write_schema(
        "transition", schema.transitions, ["applicant_id", "stage_name", "created_at"]
    )

    for transition in list_transitions(api_key, applicant_ids):
        with Transformer() as transformer:
            record = transformer.transform(transition, schema.transitions)

        singer.write_record("transition", record)


def list_transitions(api_key, applicant_ids):
    session = create_requests_session({"X-ACCESS-TOKEN": api_key})

    for index, applicant_id in enumerate(applicant_ids):
        data = {}
        count = 0
        while count < MAX_ATTEMPTS:
            try:
                url = TRANSITIONS_URL.format(id=applicant_id)
                response = session.get(url)
                response.raise_for_status()

                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                LOGGER.info(e)
                LOGGER.info("Camping until wanted level cleared")
                time.sleep(15)
                count = count + 1

        for transition in data.get("transitions", []):
            yield {
                "applicant_id": applicant_id,
                "stage_id": transition["stage_id"],
                "stage_name": transition["stage_title"],
                "created_at": transition["created_at"],
            }

        if (index + 1) % 10 == 0:
            LOGGER.info("Added transitions for %i applicants", index + 1)

def write_funnels(config):
    funnel_ids = []

    api_key = config["fountain_api_token"]
    singer.write_schema("funnel", schema.funnels, ["id"])

    for funnel in list_funnels(api_key):
        funnel_ids.append(funnel["id"])

        with Transformer() as transformer:
            record = transformer.transform(funnel, schema.funnels)

        singer.write_record("funnel", record)

    return funnel_ids

def list_funnels(api_key):
    current = 1
    last = None
    i = 0

    session = create_requests_session({"X-ACCESS-TOKEN": api_key})
    while current != None and current != last and i <= MAX_PAGES:
        response = session.get(OPENINGS_URL, params={"page": current})

        response.raise_for_status()
        data = response.json()

        current = data["pagination"]["next"]
        last = data["pagination"]["last"]
        for funnel in data["funnels"]:
            yield funnel

if __name__ == "__main__":
    main()
