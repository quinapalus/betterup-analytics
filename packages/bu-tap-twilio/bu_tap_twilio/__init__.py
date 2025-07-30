#!/usr/bin/env python3

import base64
import os
import json
import pprint

import singer

from dotenv import load_dotenv

from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from twilio.rest import Client

from . import schema

if os.path.isfile('.env'):
    load_dotenv()

REQUIRED_CONFIG_KEYS = ["start_date", "account_sid", "auth_token"]
LOGGER = singer.get_logger()

# ignore because arbitrary helper function
def get_abs_path(path): # pragma: no cover
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def discover():
    raw_schemas = { at: schema.logs for at in ['events'] }
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

# ignore because this involves API call
def twilio_audit(config, activity_type): # pragma: no cover
    client = Client(config['account_sid'], config['auth_token'])

    page_token = None
    start_date = config['start_date']

    elements = client.monitor.events.list(start_date=start_date)

    for element in elements:
        yield {
            "account_sid": element.account_sid,
            "actor_sid": element.actor_sid,
            "actor_type": element.actor_type,
            "description": element.description,
            "event_data": element.event_data,
            "event_date": element.event_date.strftime("%c"),
            "event_type": element.event_type,
            "resource_sid": element.resource_sid,
            "resource_type": element.resource_type,
            "sid": element.sid,
            "source": element.source,
            "source_ip_address": element.source_ip_address,
            "url": element.url
        }



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

        records = [singer.Transformer().transform(element, schema.logs) for element in twilio_audit(config, stream['tap_stream_id'])]

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
