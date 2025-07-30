#!/usr/bin/env python3

# Mostly ripped off from the template code.

import argparse
import boto3
import io
import json
import os
import sys
import singer

from datetime import datetime
from collections.abc import MutableMapping 
from jsonschema.validators import Draft4Validator

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

def persist_lines(name, config, lines):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    files = {}
    names = {}

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

            if o['stream'] not in files:
                names[o['stream']] = f"{o['stream']}-{name}"
                files[o['stream']] = open(names[o['stream']], 'w')

            f = files[o['stream']]

            # Get schema for this record's stream
            schema = schemas[o['stream']]

            # Validate record
            validators[o['stream']].validate(o['record'])

            # If the record needs to be flattened, uncomment this line
            flattened_record = flatten(o['record'])

            f.write(json.dumps(flattened_record))
            f.write("\n")

            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    for f in files.values():
        f.close()

    return state, names

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-b', '--bucket', help='AWS Bucket (overrides AWS_S3_BUCKET if set)')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    # S3 keys out of the environment
    s3 = boto3.resource('s3')
    bucket_name = args.bucket or os.environ.get('AWS_S3_BUCKET')
    bucket = s3.Bucket(bucket_name)

    now = datetime.now().strftime('%Y%m%dT%H%M%S')
    suffix = f"-{now}"

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state, names = persist_lines(suffix, config, input)

    for name in names.values():
        logger.info(f"Uploading {name} to {bucket_name}...")
        response = bucket.upload_file(name, f"{name}")
        os.unlink(name)

        if not response:
            emit_state(state)
        else:
            raise(Exception(f"Upload to S3 failed: {response}"))


if __name__ == '__main__':
    main()
