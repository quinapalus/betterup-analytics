with accounts_history as (

    select * from {{ ref('int_sfdc__accounts_snapshot') }}
    where is_current_version and not is_deleted

),

eligible_accounts as (

    select * from {{ ref('int_account_health_eligible_accounts') }}
    --all accounts that meet criteria from here
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3141402630/Member+Populations+and+Timing+of+Metrics+in+Account+Health+2.0+Model#Excluded-Accounts-Cheatsheet

)

select
accounts_history.sfdc_account_id,

--industry driver logic
case
    when account_primary_industry_zoominfo in ('Software','Finance','Business Services',
                                               'Consumer Services','Law Firms & Legal Services',
                                               'Telecommunications')
        then 'Good'
    when account_primary_industry_zoominfo in ('Agriculture', 'Insurance', 'Construction', 'Hospitality',
                                                'Minerals & Mining')
        then 'Poor'
    else 'Okay' end as account_health_rubric_external_factors_driver_industry,

--annual revenue driver logic
case
    when annual_account_revenue_zoominfo_usd > 1000000000
        then 'Good'
    when annual_account_revenue_zoominfo_usd between 50000000 and 1000000000
        then 'Okay'
    when annual_account_revenue_zoominfo_usd < 50000000
        then 'Poor'
    else null end as account_health_rubric_external_factors_driver_annual_revenue,

--overall score calculation
case
    --special exceptions
    when annual_account_revenue_zoominfo_usd > 50000000000
        then 'Good'
    when annual_account_revenue_zoominfo_usd < 10000000
        then 'Poor'
    when annual_account_revenue_zoominfo_usd is null or account_primary_industry_zoominfo is null
        then 'Not Enough Data'

    --normal logic
    when account_health_rubric_external_factors_driver_industry = 'Good'
         and account_health_rubric_external_factors_driver_annual_revenue = 'Good'
        then 'Good'
    when annual_account_revenue_zoominfo_usd < 100000000
        and account_health_rubric_external_factors_driver_industry = 'Poor'
        then 'Poor'
    else 'Okay' end as account_health_rubric_external_factors_overall_score

from accounts_history
inner join eligible_accounts
    on eligible_accounts.sfdc_account_id = accounts_history.sfdc_account_id
