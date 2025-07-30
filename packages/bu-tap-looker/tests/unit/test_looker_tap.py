import filecmp
import mock
import os
import sys
from fixtures import fixtures
from utils import testing_looker_audit
from bu_tap_looker import sync, discover

def test_sync():
    with mock.patch("bu_tap_looker.looker_audit", testing_looker_audit) as _:
        test_output = "tests/unit/test.out"
        expected_output = "tests/unit/fixtures/expected.out"

        with open(test_output, "w") as sys.stdout:
            catalog = discover()
            sync(fixtures.get_mock_config, None, catalog)
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
                    'type':'object',
                    'additionalProperties':False,
                    'properties':{
                    'event.created_time':{
                        'type':[
                            'string',
                            'null',
                        ],
                        'format':'date-time'
                    },
                    'event.id':{
                        'type':[
                            'string',
                            'null',
                        ]
                    },
                    'event.is_admin':{
                        'type':[
                            'null',
                            'string',
                        ]
                    },
                    'event.is_api_call':{
                        'type':[
                            'null',
                            'string',
                        ]
                    },
                    'event.name':{
                        'type':[
                            'string',
                            'null',
                        ]
                    },
                    'user.email':{
                        'type':[
                            'string',
                            'null',
                        ]
                    },
                    'user.id':{
                        'type':[
                            'string',
                            'null',
                        ]
                    },
                    'role.name':{
                        'type':[
                            'string',
                            'null',
                        ]
                    }
                    }
                },
                'metadata':{
                    'selected':True
                },
                'key_properties':[]
            }
        ]
    }
    assert(catalog == expected_catalog)
