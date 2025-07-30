#!python3

import datetime
import json
import os

from dotenv import load_dotenv

load_dotenv(verbose=True)

def create_twilio_configs():
  twilio_configs = {
    'account_sid': os.getenv('TWILIO_ACCOUNT_SID'),
    'auth_token': os.getenv('TWILIO_AUTH_TOKEN'),
    'start_date': (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime('%Y-%m-%dT%H:%M:%SZ')
  }
  with open('twilio.json', 'w') as f:
    f.write(json.dumps(twilio_configs))

if __name__ == "__main__":
    create_twilio_configs()  # pylint: disable=no-value-for-parameter
