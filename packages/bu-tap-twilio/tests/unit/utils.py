from fixtures.fixtures import get_mock_twilio_events

def testing_twilio_audit(config, activity_type):
    for element in get_mock_twilio_events():
        yield element
