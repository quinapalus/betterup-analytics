#!/usr/bin/env python3

import base64
import os
import json
import pprint

import looker_sdk
import singer

from dotenv import load_dotenv

from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from . import schema

if os.path.isfile('.env'):
    load_dotenv()

looker = looker_sdk.init31()

REQUIRED_CONFIG_KEYS = ["start_date"]
LOGGER = singer.get_logger()

# not covering because basic helper function
def get_abs_path(path): # pragma: no cover
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def discover():
    raw_schemas = { 'events': schema.events }
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

# not covering because function uses API call
def looker_audit(config, activity_type): # pragma: no cover
    start_date = config['start_date']

    try:
        body = looker_sdk.models.WriteQuery(
            model="i__looker",
            view="event",
            fields=["event.created_time", "event.id", "event.is_admin", "event.is_api_call", "event.name",
                    "user.email", "user.id",
                    "role.name"],
            filters={ "event.created_time": f"after {start_date}" },
        )

        elements = json.loads(looker.run_inline_query('json', body))
    except Exception as e:
        LOGGER.error(e)
        raise e

    for element in elements:
        yield element

def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog['streams']:
        LOGGER.info("Syncing stream:" + stream['tap_stream_id'])

        singer.write_schema(
            stream_name=stream['tap_stream_id'],
            schema=stream['schema'],
            key_properties=stream['key_properties'],
        )

        records = [singer.Transformer().transform(element, schema.events) for element in looker_audit(config, stream['tap_stream_id'])]

        singer.write_records(stream['tap_stream_id'], records)


@utils.handle_top_exception(LOGGER)
def main(): # pragma: no cover
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        json.dumps(catalog)
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
