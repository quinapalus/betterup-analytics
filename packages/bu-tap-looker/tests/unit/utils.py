from fixtures.fixtures import get_mock_looker_events

def testing_looker_audit(config, activity_type):
    for element in get_mock_looker_events():
        yield element
