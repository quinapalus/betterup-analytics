# bu-tap-looker

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Looker
- format of data pulled from looker:
```
{
   "event.created_time":<event.created_time>,
   "event.id":<event.id>,
   "event.name":<event.name>,
   "user.email":<user.email>,
   "user.id":<user.id>,
   "role.name":<role.name>"
}
```
- format of data outputted from tap:
```
{
   "type":"RECORD",
   "stream":"events",
   "record":{
      "event.created_time":<event.created_time>,
      "event.id":<event.id>,
      "event.name":<event.name>,
      "user.email":<user.email>,
      "user.id":<user.id>,
      "role.name":<role.name>
   }
}
```
- Extracts Audit logs
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2018 Stitch
