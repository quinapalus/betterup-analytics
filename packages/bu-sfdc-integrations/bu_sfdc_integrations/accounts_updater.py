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


find_accounts_to_update = """
SELECT
    sfdc_account_id,
    account_health_rubric_external_factors_overall_score,
    account_health_rubric_stakeholder_alignment_overall_score,
    account_health_rubric_adoption_overall_score,
    (percent_invited_members_activated_past_30_days_adoption_rubric*100)::varchar as percent_invited_members_activated_past_30_days_adoption_rubric,
    (percent_members_with_1_session_past_30_days_adoption_rubric*100)::varchar as percent_members_with_1_session_past_30_days_adoption_rubric,
    account_health_rubric_engagement_overall_score,
    average_sessions_per_member_per_month_engagement_rubric::varchar as average_sessions_per_member_per_month_engagement_rubric,
    (percent_of_members_with_1_reflection_point_engagement_rubric*100)::varchar as percent_of_members_with_1_reflection_point_engagement_rubric,
    account_health_rubric_nps_overall_score,
    member_net_promoter_score_nps_rubric::varchar as member_net_promoter_score_nps_rubric,
    account_health_rubric_behavorial_overall_score,
    (thriving_average_percent_growth_from_reference_behavioral_rubric*100)::varchar as thriving_average_percent_growth_from_reference_behavioral_rubric,
    (inspiring_average_percent_growth_from_reference_behavioral_rubric*100)::varchar as inspiring_average_percent_growth_from_reference_behavioral_rubric,
    (outcome_average_percent_growth_from_reference_behavioral_rubric*100)::varchar as outcome_average_percent_growth_from_reference_behavioral_rubric,
    (mindset_average_percent_growth_from_reference_behavioral_rubric*100)::varchar as mindset_average_percent_growth_from_reference_behavioral_rubric,
    overall_account_health_categorical_score_csm_risk_flag,
    overall_weighted_average_score::varchar as overall_weighted_average_score
FROM sfdc_account_health_fields;
/*
The *100 calculations are there becuase of how the Salesforce API interprets percentages. For example, the Salesforce API interprets 0.35 as 0.35% rather than 35%.
The *100 does this multiplication so that the Salesforce API updates fields correctly. 
*/
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
        database="analytics",
        schema="salesforce_service",
        role=os.environ['SNOWFLAKE_SALESFORCE_ROLE']
    ))
    connection = engine.connect()

    try:
        results = connection.execute(find_accounts_to_update).fetchall()
    finally:
        connection.close()
        engine.dispose()

    logger.info(f'Found {len(results)} Salesforce Accounts to update')

    # transform raw records into SFDC Account format
    account_updates = [
        {
            'Id': result.sfdc_account_id,
            'Final_Account_Health_Score__c': result.overall_account_health_categorical_score_csm_risk_flag,
            'External_Factors_Score__c': result.account_health_rubric_external_factors_overall_score,
            'Stakeholder_Alignment_Score__c': result.account_health_rubric_stakeholder_alignment_overall_score,
            'Adoption_Score__c': result.account_health_rubric_adoption_overall_score,
            'Activated_of_Invited__c': result.percent_invited_members_activated_past_30_days_adoption_rubric,
            'Completed_1st_Session_of_Activated__c': result.percent_members_with_1_session_past_30_days_adoption_rubric,
            'Engagement_Score__c': result.account_health_rubric_engagement_overall_score,
            'Average_Sessions_per_Member_per_Month__c': result.average_sessions_per_member_per_month_engagement_rubric,
            'Completed_at_least_1_RP__c': result.percent_of_members_with_1_reflection_point_engagement_rubric,
            'Sentiment_Score__c': result.account_health_rubric_nps_overall_score,
            'Member_NPS_Number__c': result.member_net_promoter_score_nps_rubric,
            'Behavioral_Outcomes_Score__c': result.account_health_rubric_behavorial_overall_score,
            'Average_Thriving_Growth__c': result.thriving_average_percent_growth_from_reference_behavioral_rubric,
            'Average_Inspiring_Growth__c': result.inspiring_average_percent_growth_from_reference_behavioral_rubric,
            'Average_Outcomes_Growth__c': result.outcome_average_percent_growth_from_reference_behavioral_rubric,
            'Average_Mindsets_Growth__c': result.mindset_average_percent_growth_from_reference_behavioral_rubric,
            'Overall_Account_Health_Score__c': result.overall_weighted_average_score
        } for result in results
    ]

    # Chunk up the records to avoid Salesforce bulk API limits
    # More info at: https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm
    group_size = 200
    for i, group in enumerate(grouper(account_updates, group_size)):
        logger.info(f'Updating group #{i}')
        results = salesforce.bulk.Account.update(account_updates)
        log_failures(results)
        time.sleep(1)

    logger.info(f'Updating final group')
    # grouper drops the last group if the iterator does not have
    # enough elements to fill the final group. So at most
    # there are (group_size - 1) accounts left to update.
    # We'll go ahead an update the last group_size number of accounts
    # to cover the final group. It doesn't really matter that
    # there is overlap with previously updated accounts as
    # Salesforce will happily update them again.
    final_group = account_updates[-group_size:]
    results = salesforce.bulk.Account.update(final_group)
    log_failures(results)


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
