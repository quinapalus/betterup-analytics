#!python3

import datetime
import json
import os

from dotenv import load_dotenv

load_dotenv()

def create_looker_configs():
  looker_configs = {
    'start_date': (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S')
  }
  with open('looker.json', 'w') as f:
    f.write(json.dumps(looker_configs))

if __name__ == "__main__":
    create_looker_configs()  # pylint: disable=no-value-for-parameter
