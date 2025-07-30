# What the job does
This singer tap pulls a list of events from the twilio API, and uploads them to the `bu-s3-twilio-logs` bucket. This folder contains the logic for the tap, and we use the singer target under `packages/target-s3` to upload data to s3. See `bin/twilio-s3-sync` for the full bash script. 

# Input format
```
{
    "account_sid": element.account_sid,
    "actor_sid": element.actor_sid,
    "actor_type": element.actor_type,
    "description": element.description,
    "event_data": element.event_data,
    "event_date": element.event_date.strftime("%c"),
    "event_type": element.event_type,
    "resource_sid": element.resource_sid,
    "resource_type": element.resource_type,
    "sid": element.sid,
    "source": element.source,
    "source_ip_address": element.source_ip_address,
    "url": element.url
}
```

## Output format
```
{"type": "SCHEMA", "stream": "events", "schema": {"events": {"type": "object", "additionalProperties": false}}, "key_properties": []}
{"type": "RECORD", "stream": "events", "record": {"account_sid": "account_sid", "actor_sid": "actor_sid", "actor_type": "actor_type", "description": "description", "event_data": <event data json>, "event_date": "event_date", "event_type": "event_type", "resource_sid": "resource_sid", "resource_type": "resource_type", "sid": "sid", "source": "web", "source_ip_address": "source_ip", "url": "url"}}
... more RECORD rows
```

