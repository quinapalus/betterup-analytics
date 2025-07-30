#!/usr/bin/env python3

import base64
import os
import json
import pprint

import requests
import singer

from dotenv import load_dotenv

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth

from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from . import schema

if os.path.isfile('.env'):
    load_dotenv()

REQUIRED_CONFIG_KEYS = ["start_date", "client_id", "client_secret", "datacenter"]
LOGGER = singer.get_logger()
ACTIVITY_TYPES = [
    'logins',
    'session_creations',
    'session_terminations',
    'password_changes',
    'password_resets',
    'users',
    'brands',
    'role_membership_change',
    'role_permission_change',
    'user_permission_change',
    'contact_list',
    'contact',
    'directory',
    'directory_setting',
    'dashboard_usage'
]

# not covering because basic helper function
def get_abs_path(path): # pragma: no cover
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def discover():
    raw_schemas = { at: schema.logs for at in ACTIVITY_TYPES }
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


def qualtrics_audit(config, activity_type): # pragma: no cover
    host = f"{config['datacenter']}.qualtrics.com"
    types_url = '/API/v3/logs/activitytypes'
    query_url = '/API/v3/logs'
    auth_url = '/oauth2/token'

    client = BackendApplicationClient(client_id=config['client_id'])
    client.prepare_request_body(scope=['read:activity_logs'])
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=f"https://{host}{auth_url}",
                              client_id=config['client_id'],
                              client_secret=config['client_secret'])
    headers = {
        'Authorization': f"Bearer {token['access_token']}"
    }

    page_token = None
    start_date = config['start_date']
    done = False
    while not done:
        params = {
            'activityType': activity_type,
            'skipToken': page_token,
            'startDate': start_date
        }

        response = requests.get(f"https://{host}{query_url}",
                                headers=headers,
                                params=params)
        result = response.json().get('result', {})
        elements = result.get('elements', [])
        page_token = result.get('nextPage')
        done = True if page_token is None else False

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

        records = [singer.Transformer().transform(element, schema.logs) for element in qualtrics_audit(config, stream['tap_stream_id'])]

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
