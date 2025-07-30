def get_mock_looker_events():
    return [
        {
            "event.created_time": "2023-01-01T00:00:00.000000Z",
            "event.id": "event_id_1",
            "event.name": "event_name_1",
            "user.email": "test@betterup.co",
            "user.id": "test1",
            "role.name": "test1"
        },
        {
            "event.created_time": "2023-01-01T00:00:00.000000Z",
            "event.id": "event_id_2",
            "event.name": "event_name_2",
            "user.email": "test@betterup.co",
            "user.id": "test2",
            "role.name": "test2"
        },
        {
            "event.created_time": "2023-01-01T00:00:00.000000Z",
            "event.id": "event_id_3",
            "event.name": "event_name_3",
            "user.email": "test@betterup.co",
            "user.id": "test3",
            "role.name": "test3"
        }
    ]

def get_mock_config():
    return {"start_date": "2023-01-01 00:00:00"}
