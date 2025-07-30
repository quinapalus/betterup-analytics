def get_mock_qualtric_events(config, activity_type):
    return [
        {
            "id":"test1",
            "timestamp":"test_timestamp1",
            "datacenter":"test_datacenter1",
            "source":"source1",
            "descriptor": {
                "brandId":"brandId1",
                "userId":"userId1",
                "agentUserId":"agentUserId1",
                "agentSessionId":"agentSessionId1",
                "sessionId":"sessionId1",
                "startDate":"startDate1",
                "endDate":"endDate1",
                "reason":"reason1",
                "inactivityTimeout":"inactivityTimeout1",
                "maximumLength":"maximumLength1"
            }
        },
        {
            "id":"test2",
            "timestamp":"test_timestamp2",
            "datacenter":"test_datacenter2",
            "source":"source2",
            "descriptor": {
                "brandId":"brandId2",
                "userId":"userId2",
                "agentUserId":"agentUserId2",
                "agentSessionId":"agentSessionId2",
                "sessionId":"sessionId2",
                "startDate":"startDate2",
                "endDate":"endDate2",
                "reason":"reason2",
                "inactivityTimeout":"inactivityTimeout2",
                "maximumLength":"maximumLength2"
            }
        },
        {
            "id":"test3",
            "timestamp":"test_timestamp3",
            "datacenter":"test_datacenter3",
            "source":"source3",
            "descriptor": {
                "brandId":"brandId3",
                "userId":"userId3",
                "agentUserId":"agentUserId3",
                "agentSessionId":"agentSessionId3",
                "sessionId":"sessionId3",
                "startDate":"startDate3",
                "endDate":"endDate3",
                "reason":"reason3",
                "inactivityTimeout":"inactivityTimeout3",
                "maximumLength":"maximumLength3"
            }
        }
    ]

def get_mock_config():
    return {"client_id": "test", "client_secret": "test", "datacenter": "iad1", "start_date": "2023-01-01T00:00:00Z"}


