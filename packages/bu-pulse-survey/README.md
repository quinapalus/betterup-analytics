# First Pulse Survey Sender

`bu-pulse-survey` is a package to automate the operations of
identifying members who have either (1) recently completed onboarding or
(2) recently completed their first coaching session. Qualtrics is
used to create and distribute the surveys. This package is responsible
for identifying which members to send each of the respective surveys to,
uploading that list to Qualtrics, and setting the schedule at which
to send the surveys.


# Installation

`pip` is used to install the tap for both normal and development installations.

```
pip install .      # Standard install
pip install -e .   # Development install
```

# Examples

By default `send-first-pulse-survey` will execute in a demo mode. Dummy contacts will
be uploaded to Qualtrics. `demo` should be used as an integration test when making
changes to `send-first-pulse-survey`.
```
send-first-pulse-survey demo
```

To send surveys out in limited capacity, use the `limited` option. `send-first-pulse-survey`
will send surveys to the first ten candidate members identified.
```
send-first-pulse-survey limited
```

For production, use `full` mode. Every candidate member identified will be sent an appropriate
survey.
```
send-first-pulse-survey full
```
