# bu-tap-qualtrics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Qualtrics
- Extracts Audit logs
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

Sample format of data from qualtrics API:
```
{
   "id":<id>,
   "timestamp":<timestamp>,
   "datacenter":<datacenter>,
   "source":<source>,
   "descriptor":{
      "brandId":"<brandId>,
      "userId":<userId>,
      "agentUserId":<agentUserId>,
      "agentSessionId":<agentSessionId>,
      "sessionId":<sessionId>,
      "startDate":<startDate>,
      "endDate":<endDate>,
      "reason":<reason>,
      "inactivityTimeout":<inactivityTimeout>,
      "maximumLength":<maximumLength>
   }
}
```
---

Copyright &copy; 2018 Stitch
