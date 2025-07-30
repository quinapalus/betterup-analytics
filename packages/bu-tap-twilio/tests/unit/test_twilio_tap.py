import filecmp
import mock
import os
import sys
from fixtures import fixtures
from utils import testing_twilio_audit
from bu_tap_twilio import sync, discover

def test_sync():
    with mock.patch("bu_tap_twilio.twilio_audit", testing_twilio_audit) as _:
        test_output = "tests/unit/test.out"
        expected_output = "tests/unit/fixtures/expected.out"
        with open(test_output, "w") as sys.stdout:
            catalog = discover()
            sync(fixtures.get_mock_config(), None, catalog)
            assert(filecmp.cmp(test_output, expected_output))
    os.remove(test_output)

def test_discover():
    catalog = discover()
    expected_catalog = {
        'streams':[
            {
                'stream':'events',
                'tap_stream_id':'events',
                'schema':{
                    'events':{
                    'type':'object',
                    'additionalProperties':False
                    }
                },
                'metadata':{
                    'selected':True
                },
                'key_properties':[
                    
                ]
            }
        ]
    }
    assert(catalog == expected_catalog)
