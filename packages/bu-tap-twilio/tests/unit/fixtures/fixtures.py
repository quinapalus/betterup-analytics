def get_mock_twilio_events():
    return [
        {
            "account_sid": "account_sid1",
            "actor_sid": "actor_sid1",
            "actor_type": "actor_type1",
            "description": "description1",
            "event_data": {"testattr1": "test", "testattr2": "test"},
            "event_date": "2022-01-01T00:00:00Z",
            "event_type": "event_type1",
            "resource_sid": "resource_sid1",
            "resource_type": "resource_type1",
            "sid": "sid1",
            "source": "source1",
            "source_ip_address": "source_ip_address1",
            "url": "url1",
        },
        {
            "account_sid": "account_sid2",
            "actor_sid": "actor_sid2",
            "actor_type": "actor_type2",
            "description": "description2",
            "event_data": {"testattr1": "test", "testattr2": "test"},
            "event_date": "2022-01-01T00:00:00Z",
            "event_type": "event_type2",
            "resource_sid": "resource_sid2",
            "resource_type": "resource_type2",
            "sid": "sid2",
            "source": "source2",
            "source_ip_address": "source_ip_address2",
            "url": "url2",
        }
    ]

def get_mock_config():
    return {"account_sid": "test_account_sid", "auth_token": "test_auth_token", "start_date": "2022-01-01T00:00:00Z"}
