from configparser import ConfigParser
import datetime
import json
import logging
import os
import re
from urllib.parse import urlparse

from dotenv import load_dotenv, find_dotenv
import pandas as pd
import psycopg2
import requests
import click_log


load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def decompose_database_url(url):
    """Turn a database URL into a dictionary with the connection info.

    Args:
        url (str): Databse connection URL.

    Returns:
        dict: Contents of URL separated into key-value pairs.
    """
    result = urlparse(url)
    config = {
        "host": result.hostname,
        "port": result.port,
        "user": result.username,
        "password": result.password,
        "dbname": result.path[1:],
    }

    return config


class DBPuller(object):
    """ Connect to DB and pull Member data. """

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.cursor = None

    @staticmethod
    def date_to_str(d):
        try:
            return d.replace(tzinfo=datetime.timezone.utc).isoformat()
        except AttributeError:
            return "NaT"

    def connect(self):
        """ Connect to the PostgreSQL database server. """
        # read connection parameters
        params = decompose_database_url(self.url)

        # connect to the PostgreSQL server
        logger.info('Connecting to the PostgreSQL database...')
        self.connection = psycopg2.connect(**params)

        # create a cursor
        self.cursor = self.connection.cursor()

        logger.info("Connected.")

    def close(self):
        self.connection.close()
        self.connection = None
        self.cursor = None

    def pull(self, start_date):
        """Pull data from database.
           start_date must be in MM/DD/YYYY format."""
        q = f"""WITH
                onboarding_complete AS (
              SELECT DISTINCT ON (member_id)
                member_id,
                created_at AS onboarding_complete_at
              FROM app.app_sessions
              ORDER BY member_id, created_at ASC
              ),
              first_scheduled_session AS (
              SELECT DISTINCT ON (member_id)
                member_id,
                starts_at,
                ends_at
              FROM app.app_sessions
              WHERE canceled_at IS NULL AND NOT missed
              ORDER BY member_id, starts_at ASC
              ),
              first_completed_session AS (
              SELECT DISTINCT ON (member_id)
                member_id,
                event_at
              FROM app.app_billable_events
              WHERE event_type = 'completed_sessions'
              ORDER BY member_id, event_at ASC
              )


            SELECT
              met.member_id,
              smi.email,
              smi.first_name,
              smi.last_name,
              oc.onboarding_complete_at,
              ss.starts_at,
              ss.ends_at,
              cs.event_at,
              t.deployment_type,
              met.track_id,
              t.name AS track
            FROM analytics.dei_member_engagement_by_track AS met
            INNER JOIN restricted.smi_member_info AS smi ON met.member_id = smi.member_id
            INNER JOIN app.app_tracks AS t ON met.track_id = t.track_id
            INNER JOIN onboarding_complete AS oc ON met.member_id = oc.member_id
            INNER JOIN first_scheduled_session AS ss ON met.member_id = ss.member_id
            LEFT OUTER JOIN first_completed_session AS cs ON met.member_id = cs.member_id
            -- only include "open" members. Members can only be open on one track at a time
            WHERE ended_at IS NULL
            AND oc.onboarding_complete_at > '{start_date}'
            AND t.deployment_type = 'standard'
            """
        return pd.read_sql_query(q, con=self.connection)

    def process_records(self, df):
        """Applies basic data transformations - rename, datetime-to-string, regex,
           in preparation for calling Qualtrics API, which requires specific formats."""

        col_names = {"first_name": "firstName", "last_name": "lastName",
                     "event_at": "confirmed_start", "starts_at": "planned_start",
                     "ends_at": "planned_end"}
        cols = ["firstName", "lastName", "email", "language", "onboarding_complete_at",
                "planned_start", "planned_end", "confirmed_start"]
        records = {"contacts": df.rename(columns=col_names)
                                 .assign(language="EN")[cols]
                                 .to_dict(orient="records")}
        for i, rec in enumerate(records["contacts"]):
            rec["embeddedData"] = dict(
                planned_start=self.date_to_str(rec["planned_start"]),
                planned_end=self.date_to_str(rec["planned_end"]),
                confirmed_start=self.date_to_str(rec["confirmed_start"]),
                # onboarding_complete_at has decimal time (to microseconds) so truncate to seconds
                onboarding_complete_at=re.sub(r"\..*\+", r"+",
                                              self.date_to_str(rec["onboarding_complete_at"]))
            )
            to_delete = ["planned_start", "planned_end", "confirmed_start",
                         "onboarding_complete_at"]
            for deleted in to_delete:
                del rec[deleted]
            records["contacts"][i] = rec

        return records


class Qualtrics(object):
    """ Holds universal parameters required of all Qualtrics API calls. """

    def __init__(self):
        self.api_token = os.environ.get("QUALTRICS_API_TOKEN")
        # test id: ML_dou8jViOzjBPDJr; live id: ML_6gOiIeUYkD1X7dr
        self.contact_list_id = os.environ.get("FIRST_PULSE_CONTACT_LIST_ID")
        self.library_id = os.environ.get("FIRST_PULSE_LIBRARY_ID")
        self.headers = {'X-API-TOKEN': self.api_token}

    @staticmethod
    def date_to_str(d):
        return d.replace(tzinfo=datetime.timezone.utc).isoformat()


class ContactGetter(Qualtrics):
    """ Get information about each Contact in the contact list
        represented by contact_list_id. """

    def __init__(self):
        super().__init__()
        endpoint_head = "https://co1.qualtrics.com/API/v3/mailinglists/"
        self.endpoint = f"{endpoint_head}{self.contact_list_id}/contacts"
        self.contacts = list()

    def get(self):
        # reset contacts list on each call to get()
        self.contacts = list()
        current_endpoint = self.endpoint
        while current_endpoint is not None:
            r = requests.get(current_endpoint, headers=self.headers)
            if r.status_code != 200:
                self.contacts = None
                logger.error(f"ERROR getting contacts: {r.json()}")
                return None

            if "nextPage" in r.json()["result"].keys():
                current_endpoint = r.json()["result"]["nextPage"]
            else:
                current_endpoint = None

            self.contacts += r.json()["result"]["elements"]

        if len(self.contacts) > 0:
            logger.info("Contacts retrieved from Qualtrics.")
        else:
            logger.error("Attempted to retrieve contacts from Qualtrics, "
                  "but no contacts exist!")


class ContactUploader(Qualtrics):
    """ Uploads member data to Qualtrics mailing list in JSON format.
        See https://api.qualtrics.com/docs/create-contacts-import for API parameters. """

    def __init__(self, contacts, contacts_file=None):
        super().__init__()
        endpoint_head = "https://co1.qualtrics.com/API/v3/mailinglists/"
        self.endpoint = f"{endpoint_head}{self.contact_list_id}/contactimports"
        self.headers.update({'Content-Type': 'application/json'})
        self.upload_progress = 0
        self.prog_id = None
        self.cgetter = ContactGetter()
        self.cgetter.get()
        # catch error if no contacts are submitted
        self.contacts = contacts.copy()
        if not contacts:
            self._populate_contacts(contacts_file)
        self.new_contacts, self.updates = self._check_updates()
        self.invite_list = list()

    def _check_updates(self):
        logger.info('Checking for updates')
        updates = list()
        new_contacts = list()
        new_emails = [c["email"] for c in self.contacts["contacts"]]
        existing_users = [{"email": user["email"],
                           "id": user["id"],
                           "embeddedData": user["embeddedData"]}
                          for user in self.cgetter.contacts
                          if user["email"] in new_emails]

        if len(existing_users) == 0:
            return self.contacts["contacts"], []
        existing_emails = [u["email"] for u in existing_users]

        for i, new_entry in enumerate(self.contacts["contacts"]):
            if new_entry["email"] in existing_emails:
                # use next() here because it should only ever be a single match
                matched_user = next(u for u in existing_users
                                    if u["email"] == new_entry["email"])
                # oct 2018 note:
                # for current deployment, only ``confirmed_start`` is mutable
                # this value will go from 'NaT' to datetime, post first-session
                # for future versions, any mutable field should be checked here
                for field in ["confirmed_start"]:
                    if new_entry["embeddedData"][field] != \
                            matched_user["embeddedData"][field]:
                        new_entry["id"] = matched_user["id"]
                        updates.append(new_entry)
                        break
            else:
                new_contacts.append(new_entry)
        return new_contacts, updates

    def _populate_contacts(self, contacts_file):
        if contacts_file is not None:
            self.contacts = json.load(open(contacts_file))
        else:
            raise AttributeError(
                """ContactUploader instances must be initialized with either
                   the contacts or contacts_file parameter."""
            )

    def upload(self):
        contacts = self.new_contacts + self.updates
        if not contacts:
            logger.warning('Skipping contact upload since there were no contacts identified')
            return

        payload = {"contacts": self.new_contacts + self.updates}

        r = requests.post(self.endpoint, headers=self.headers, json=payload)

        if r.status_code != 200:
            logger.error(f'Contact upload failed: {r.json()}')
            logger.error(payload)
            return None

        self.prog_id = r.json()["result"]["id"]
        prev_progress = 0
        while self.upload_progress != 100:
            self.upload_progress = self._check_upload_status()
            if self.upload_progress != prev_progress:
                logger.info(f"Contact list upload progress: {self.upload_progress:.2f}%")
            prev_progress = self.upload_progress
        logger.info("Contact list upload complete.")
        self.invite_list, self.updates = self._sync_contacts()

    def _check_upload_status(self):
        endpoint_head = "https://co1.qualtrics.com/API/v3/mailinglists/"
        endpoint = f"{endpoint_head}{self.contact_list_id}/contactimports/{self.prog_id}"
        r = requests.get(endpoint, headers=self.headers)

        if r.status_code != 200:
            logger.error('Error checking upload status')
            return None

        return r.json()["result"]["percentComplete"]

    def _sync_contacts(self):
        """ Pulls newly-updated list of contacts from ContactGetter.
            Stores list in invite_list for use with InviteScheduler. """
        # get updated contact list, post-upload()
        self.cgetter.get()
        new_contact_emails = [c["email"] for c in self.new_contacts]
        update_emails = [u["email"] for u in self.updates]
        invite_list = list()
        updates = list()
        for entry in self.cgetter.contacts:
            if entry["email"] in new_contact_emails:
                invite_list.append(entry)
            elif entry["email"] in update_emails:
                updates.append(entry)
        return invite_list, updates


class InviteScheduler(Qualtrics):
    """ Schedules invites to take survey, based on date/time indicated in Contact entry.
        The date/time of the post-session invite is always fixed to the end-of-session
        date/time.  The date/time of the pre-session invite can be fixed either to the
        completion of onboarding or to the date set for the initial session.
        23 Oct 2018 status: Pre-session invite fixed to completion of onboarding."""

    def __init__(self, pre_anchor, post_anchor, pre_offset, post_offset):
        super().__init__()
        self.headers.update({'Content-Type': 'application/json'})
        self.endpoint = "https://co1.qualtrics.com/API/v3/distributions"
        self.pre_anchor = pre_anchor
        self.post_anchor = post_anchor
        self.pre = dict(message="MS_0wA2SI3uvKTvsfH",
                        survey="SV_dakIiDRbSf5X4oZ",
                        subject="Hey! Take a survey before you do coaching.",
                        offset=pre_offset,
                        offset_cat="pre")
        self.post = dict(message="MS_3JK7CHmL6R5lsKp",
                         survey="SV_2mGZYhRXaPWM9MN",
                         subject="Now it's time to follow up.",
                         offset=post_offset,
                         offset_cat="post")

    def _check_active(self, survey_id):
        """ Activates Qualtrics survey. This is necessary in order to set up Distributions.
            If survey is already active, this function will have no effect. """
        r = requests.put(
            f"https://co1.qualtrics.com/API/v3/surveys/{survey_id}",
            headers=self.headers,
            json={"isActive": True}
        )
        if r.status_code != 200:
            logger.error(f"ERROR activating survey {survey_id}; {r.json()}")

    def schedule(self, new_contacts, updates):
        """ Schedules invitations to be sent based on first session
            date/time indicated in Contact entry."""

        for member in new_contacts:
            self._schedule_at("pre", member)
            self._schedule_at("post", member)

        for member in updates:
            # oct 2018: only "post" can be updated, "pre" does not change
            # self._schedule_at("pre", member, reschedule=True)
            # also, turn off reschedule flag - no need now, given that
            # the only update can be a post invite where before there was none
            self._schedule_at("post", member, reschedule=False)

    def _schedule_at(self, when, member, reschedule=False):
        """ Schedules an email invitation to take a survey. The ``when``
            parameter determines whether the invitation is sent before or after
            the first session. """
        dist_id_str = f"{when}_distribution_id"

        if reschedule:
            if not self._clear_previous_schedule(member, when, dist_id_str):
                return None
        else:
            if dist_id_str in member["embeddedData"].keys():
                logger.errror(f"WARNING: Member {member['id']} is listed under new",
                              f"contacts but already has {when.upper()} invite set.")
                return None

        if when == "pre":
            dt = pd.to_datetime(member['embeddedData'][self.pre_anchor])
            params = self.pre
        elif when == "post":
            if member["embeddedData"]["confirmed_start"] == "NaT":
                print(f'Member {member["id"]} has not yet had a confirmed',
                      'first session. First session is scheduled for',
                      f'{member["embeddedData"]["planned_start"]}.',
                      'No post-session invite will be set for now.')
                return None
            dt = pd.to_datetime(member['embeddedData'][self.post_anchor])
            params = self.post
        else:
            raise ValueError("``when`` must be one of: 'pre', 'post'.")

        self._check_active(params["survey"])
        self._call_scheduler(when, dt, params, member)

    def _clear_previous_schedule(self, member, when, dist_id_str):
        if dist_id_str not in member["embeddedData"].keys():
            print(f"WARNING: Member {member['id']} is listed in updates",
                  f"but has no existing {when.upper()} invite set.")
            return False
        dist_id = member["embeddedData"][dist_id_str]
        endpoint = f"https://co1.qualtrics.com/API/v3/distributions/{dist_id}"

        r = requests.delete(endpoint, headers=self.headers)

        if r.status_code != 200:
            print(f"ERROR deleting old mailing distribution ({dist_id})",
                  f"for Member {member['id'].upper()}; {r.json()}")
            return False
        else:
            print(f"Removed existing mailing distribution for",
                  f"Member {member['id'].upper()}")
        return True

    def _update_contact(self, contact_id, dist_id, when):
        """ Update member contact info with mailing distribution id. """
        endpoint = f"https://co1.qualtrics.com/API/v3/mailinglists/{self.contact_list_id}/contacts/{contact_id}"
        payload = {"embeddedData": {f"{when}_distribution_id": dist_id}}
        r = requests.put(endpoint, headers=self.headers, json=payload)
        if r.status_code != 200:
            print("ERROR updating contact with mailing distribution id",
                  f"for Member {contact_id.upper()}; {r.json()}")
        else:
            print(f"Contact list for Member {contact_id.upper()} updated",
                  f"with ({when.upper()}) mailing distribution id.")

    def _call_scheduler(self, when, dt, params, member):
        send_dt = self.date_to_str(dt + pd.Timedelta(params["offset"]))
        # todo: move json to yaml
        json_data = {
            "surveyLink": {
                "surveyId": params["survey"],
                "type": "Individual"
            },
            "header": {
                "fromEmail": "info@betterup.co",
                "fromName": "BetterUp",
                "replyToEmail": "info@betterup.co",
                "subject": params["subject"]
            },
            "message": {
                "libraryId": self.library_id,
                "messageId": params["message"]
            },
            "recipients": {
                "mailingListId": self.contact_list_id,
                "contactId": member["id"]
            },
            "sendDate": send_dt
        }

        r = requests.post(self.endpoint, headers=self.headers, json=json_data)

        if r.status_code != 200:
            print(f"ERROR ({params['offset_cat']})",
                  f"for Member {member['id'].upper()}; {r.json()}")

        else:
            dist_id = r.json()["result"]["id"]
            self._update_contact(member["id"], dist_id, when)
            print(f"Invite ({params['offset_cat']}) for Member",
                  f"{member['id']} scheduled successfully.")
