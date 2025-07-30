from fixtures.fixtures import get_mock_qualtric_events

def testing_qualtrics_audit(config, activity_type):
    for element in get_mock_qualtric_events(config, activity_type):
        yield element
