# target-s3

This is a [Singer](https://singer.io) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Each stream flowing out of the tap is sent to its own file, which just spools out the structured JSON data.
---

Copyright &copy; 2018 Stitch
