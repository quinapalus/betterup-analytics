import click
import click_log
import logging
import os
import requests
import time
from simple_salesforce import Salesforce
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from urllib import parse


logger = logging.getLogger(__name__)
click_log.basic_config(logger)
console = logging.StreamHandler()
formatter = logging.Formatter('[bu-sfdc-integration] %(levelname)s: %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


find_contacts_to_update = """
SELECT
    DISTINCT sfdc_contact_id,
    app_email, 
    app_first_name, 
    app_last_name,
    sfdc_is_current_member,
    sfdc_is_past_member,
    sfdc_is_program_admin,
    app_is_current_member,
    app_is_past_member,
    app_is_program_admin,
    coalesce(sfdc_account_id, app_sfdc_account_id) AS account_id
FROM salesforce_service.sfdc_contacts_global_roles
WHERE values_differ_from_app
AND NOT sfdc_is_deleted 
AND NOT app_is_deactivated
AND app_is_confirmed
AND sfdc_account_type IN ('Customer','Pilot Customer')
AND (app_is_program_admin OR sfdc_is_program_admin);
"""


@click.command()
@click_log.simple_verbosity_option()
def main():
    salesforce_auth_data = {
        "grant_type": "password",
        "client_id": os.environ['SALESFORCE_CLIENT_ID'],
        "client_secret": os.environ['SALESFORCE_CLIENT_SECRET'],
        "username": os.environ['SALESFORCE_USERNAME'],
        "password": os.environ['SALESFORCE_PASSWORD'] + os.environ['SALESFORCE_SECURITY_TOKEN']
    }
    response = requests.post(
        "https://" + os.environ['SALESFORCE_HOST'] + "/services/oauth2/token",
        data=salesforce_auth_data
    )
    if response.status_code != 200:
        raise Exception(f'Failed to retrieve OAuth token from Salesforce: {response.text}')

    access_token = response.json().get("access_token")
    instance_url = response.json().get("instance_url")

    salesforce = Salesforce(
        instance_url=instance_url,
        session_id=access_token
    )

    # connect to salesforce data warehouse
    engine = create_engine(URL(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_SALESFORCE_USER'],
        # quote_plus handles special characters in passwords otherwise the connection string may pass an incorrect password
        password=parse.quote_plus(os.environ['SNOWFLAKE_SALESFORCE_USER_PWD']),
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database='ANALYTICS',
        schema='SALESFORCE_SERVICE',
        role='SALESFORCE_SERVICE'
    ))
    connection = engine.connect()

    try:
        results = connection.execute(find_contacts_to_update).fetchall()
    finally:
        connection.close()
        engine.dispose()

    logger.info(f'Found {len(results)} Salesforce Contacts to update')

    # transform raw records into sfdc Contact format
    # account_id gives precedence to the account id that we see in SFDC for the contact
    contact_updates = [
        {
            'Id': result.sfdc_contact_id,
            'Current_User__c': result.app_is_current_member,
            'Past_User__c': result.app_is_past_member,
            'Program_Admin__c': result.app_is_program_admin,
            'Email': result.app_email,
            'FirstName': result.app_first_name,
            'LastName': result.app_last_name,
            'AccountId':result.account_id
        } for result in results
    ]

    # Chunk up the records to avoid Salesforce bulk API limits
    # More info at: https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm
    group_size = 200
    for i, group in enumerate(grouper(contact_updates, group_size)):
        logger.info(f'Updating group #{i}')
        # Upsert logic creates new contacts if there isn't a matching one existing in Salesforce
        results = salesforce.bulk.Contact.upsert(contact_updates,'Email')
        log_failures(results)
        time.sleep(1)

    logger.info(f'Updating final group')
    # grouper drops the last group if the iterator does not have
    # enough elements to fill the final group. So at most
    # there are (group_size - 1) contacts left to update.
    # We'll go ahead an update the last group_size number of contacts
    # to cover the final group. It doesn't really matter that
    # there is overlap with previously updated contacts as
    # Salesforce will happily update them again.
    final_group = contact_updates[-group_size:]
    results = salesforce.bulk.Contact.upsert(final_group,'Email')
    log_failures(results)
    logger.info(f'Done.')


def log_failures(results):
    for result in results:
        if not result['success']:
            message = result['errors'][0]['message']
            logger.warning(f'Update failed: {message}')


def grouper(iterable, n):
    """Collect data into fixed-length chunks or blocks

    Modified from: https://docs.python.org/3.7/library/itertools.html
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip(*args)
