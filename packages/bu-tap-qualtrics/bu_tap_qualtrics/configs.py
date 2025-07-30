#!python3

import datetime
import json
import os

from dotenv import load_dotenv

load_dotenv()

def create_qualtrics_configs():
  qualtrics_configs = {
    'client_id': os.getenv('QUALTRICS_CLIENT_ID'),
    'client_secret': os.getenv('QUALTRICS_CLIENT_SECRET'),
    'datacenter': os.environ.get('QUALTRICS_DATACENTER', 'iad1'),
    'start_date': (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime('%Y-%m-%dT%H:%M:%SZ')
  }
  with open('qualtrics.json', 'w') as f:
    f.write(json.dumps(qualtrics_configs))

if __name__ == "__main__":
    create_qualtrics_configs()  # pylint: disable=no-value-for-parameter
