import argparse
import datetime
import json
import logging
import re

import arrow
import click
import click_log
import pandas as pd

from . import scheduling as sched


logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def test_first_upload():
    test_import = json.loads(
        """{"contacts":[
          {
            "firstName": "Andrew",
            "lastName": "Reece",
            "email": "andrewreece@gmail.com",
            "language": "EN",
            "embeddedData": {"planned_start":"2019-01-10T17:30:00+00:00",
                             "planned_end": "2019-01-10T18:15:00+00:00",
                             "confirmed_start": "2019-01-10T17:30:00+00:00",
                             "onboarding_complete_at": "2019-01-08T12:47:36+00:00"}
          },
          {
            "firstName": "Joe",
            "lastName": "Smith",
            "email": "katie.saulsgiver@betterup.co",
            "language": "EN",
            "embeddedData": {"planned_start":"2019-01-09T23:30:00+00:00",
                             "planned_end": "2019-01-09T23:55:00+00:00",
                             "confirmed_start": "NaT",
                             "onboarding_complete_at": "2019-01-07T13:47:36+00:00"}
          }
        ]}""")

    cuploader = sched.ContactUploader(contacts=test_import)
    cuploader.upload()
    assert len(cuploader.updates) == 0, "Initial upload failed, expected cuploader.updates == empty list"
    return cuploader


def test_inviter(cuploader):
    inviter = sched.InviteScheduler(pre_anchor="onboarding_complete_at",
                                    post_anchor="confirmed_start",
                                    pre_offset="3 hour",
                                    post_offset="1 hour")
    inviter.schedule(cuploader.invite_list, cuploader.updates)


def test_second_upload():
    # change second test entry to confirmed start
    test_import = json.loads(
        """{"contacts":[
          {
            "firstName": "Andrew",
            "lastName": "Reece",
            "email": "andrewreece@gmail.com",
            "language": "EN",
            "embeddedData": {"planned_start":"2019-01-10T17:30:00+00:00",
                             "planned_end": "2019-01-10T18:15:00+00:00",
                             "confirmed_start": "2019-01-10T17:30:00+00:00",
                             "onboarding_complete_at": "2019-01-08T12:47:36+00:00"}
          },
          {
            "firstName": "Joe",
            "lastName": "Smith",
            "email": "katie.saulsgiver@betterup.co",
            "language": "EN",
            "embeddedData": {"planned_start":"2019-01-09T23:30:00+00:00",
                             "planned_end": "2019-01-09T23:55:00+00:00",
                             "confirmed_start": "2019-01-09T23:30:00+00:00",
                             "onboarding_complete_at": "2019-01-07T13:47:36+00:00"}
          }
        ]}""")

    expected = {'firstName': 'Joe',
                'lastName': 'Smith',
                'email': 'katie.saulsgiver@betterup.co'}
    cuploader = sched.ContactUploader(contacts=test_import)
    cuploader.upload()
    actual = cuploader.updates[0].copy()
    actual = {k: v for k, v in actual.items()
              if k in ["firstName", "lastName", "email"]}
    assert actual == expected,     (f"Upload update failed,\nexpected cuploader.updates == {expected}\nGot: {actual}")
    return cuploader


@click.command()
@click.argument(
    "status", type=click.Choice(['demo', 'limited', 'full']),
    default='demo',
)
@click.option("--start", default=arrow.utcnow().format('MM/DD/YYYY'))
@click.option(
    '--url', envvar='BETTERUP_LABS_URL',
    help='Database URL to retrieve application information from.'
)
def main(status, start, url):
    """
    Sends the first pulse survey to members who have:
      * completed onboarding (before survey), or
      * completed their first coaching session (after survey).

    \b
    Arguments:
        status (str): One of 'demo', 'limited', or 'full'
                         demo: run test on fake data.
                         limited: run live on first 10 db query results.
                         full: run live on all query results.
        start (str): Cut-off date (MM/DD/YYYY) to filter members.
                     Defaults to todays date.
        url (str): Database URL to retrieve application info from.

    """
    db = sched.DBPuller(url)
    db.connect()

    df = []
    logger.info(f'Pulling records after start date of {start}')
    try:
        df = db.pull(start_date=start)
    finally:
        if db.connection is not None:
            db.connection.close()

    logger.info(f"db.pull() len: {len(df)}")

    if status != "demo":

        records = db.process_records(df)

        if status == "limited":
            records = {"contacts": list(records.values())[0][:10]}
            logger.info(f"Here is the 10-record subset you are testing on: {records}")

        logger.info(f"Records len: {len(records['contacts'])}")

        cuploader = sched.ContactUploader(contacts=records)
        cuploader.upload()

        inviter = sched.InviteScheduler(pre_anchor="onboarding_complete_at",
                                        post_anchor="confirmed_start",
                                        pre_offset="3 hour",
                                        post_offset="1 hour")
        inviter.schedule(cuploader.invite_list, cuploader.updates)

    else:
        c1 = test_first_upload()
        test_inviter(c1)
        c2 = test_second_upload()
        test_inviter(c2)


if __name__ == "__main__":
    main()
