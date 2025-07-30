with accounts_history as (

    select * from {{ ref('int_sfdc__accounts_snapshot') }}
    where is_current_version and not is_deleted --only want the most recent snapshots

),

eligible_accounts as (

    select * from {{ ref('int_account_health_eligible_accounts') }}
    --all accounts that meet criteria from here
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3141402630/Member+Populations+and+Timing+of+Metrics+in+Account+Health+2.0+Model#Excluded-Accounts-Cheatsheet

)

select
accounts_history.sfdc_account_id,

--maturity curve driver logic
case
    when maturity_curve in ('3 - Strategic Transformation', '4 - Continuous Growth')
        then 'Good'
    when maturity_curve = '2 - Moments that Matter'
        then 'Okay'
    when maturity_curve = '1 - Learning Programs'
        then 'Poor'
    else null end as account_health_rubric_stakeholder_alignment_driver_maturity_curve,

--trusted advisor/ highest level champion driver logic
case
    when highest_level_champion in ('C-Suite', 'LOB Exec')
        then 'Good'
    when highest_level_champion = 'VP HR'
        then 'Okay'
    when highest_level_champion = 'Manager/Director HR'
        then 'Poor'
    else null end as account_health_rubric_stakeholder_alignment_driver_highest_level_champion,

--sphere of influence driver logic
case
    when sphere_of_influence = '4+ Champions'
        then 'Good'
    when sphere_of_influence = '1-3 Champions'
        then 'Poor'
    else null end as account_health_rubric_stakeholder_alignment_driver_sphere_of_influence,

--last qbr in last 6 months driver logic
case
    when datediff(day, last_qbr_at, current_date) <= 180
        then true
    else false end as account_health_rubric_stakeholder_alignment_driver_qbr_in_last_6_months,

--executive sponsor engagement in last 6 months driver logic
case
    when datediff(day, last_executive_sponsor_engagement_at, current_date) <= 180
        then true
    else false end as account_health_rubric_stakeholder_alignment_driver_executive_sponsor_engagement_in_last_6_months,

--success plan established driver logic
case
    when success_plan_link = 'N/A' or success_plan_link is null or length(success_plan_link) < 50
        then false
    else true end as account_health_rubric_stakeholder_alignment_driver_success_plan_established,

--sucess plan updated in past 12 months driver logic
case
    when success_plan_link = 'N/A' or success_plan_link is null or length(success_plan_link) < 50
        then false
    when datediff(day, success_plan_last_updated_at, current_date) <= 365
        then true else false end as account_health_rubric_stakeholder_alignment_driver_sucess_plan_updated_in_last_12_months,

--number of null drivers
iff(maturity_curve is null,1,0)
+ iff(highest_level_champion is null,1,0)
+ iff(sphere_of_influence is null,1,0)
+ iff(account_health_rubric_stakeholder_alignment_driver_qbr_in_last_6_months is null,1,0)
+ iff(account_health_rubric_stakeholder_alignment_driver_success_plan_established is null,1,0)
+ iff(account_health_rubric_stakeholder_alignment_driver_executive_sponsor_engagement_in_last_6_months is null,1,0)
+ iff(account_health_rubric_stakeholder_alignment_driver_sucess_plan_updated_in_last_12_months is null,1,0)
as count_of_missing_drivers,

--overall score calculation
case
--missing data scenario
    when 
        count_of_missing_drivers >= 2 
    then 'Not Enough Data'

--good scenarios
    when 
        account_health_rubric_stakeholder_alignment_driver_maturity_curve = 'Good'
        and (sphere_of_influence = '4+ Champions' or sphere_of_influence = '1-3 Champions' and account_health_rubric_stakeholder_alignment_driver_executive_sponsor_engagement_in_last_6_months)
        and highest_level_champion in ('C-Suite','LOB Exec')
    then 'Good'

    when 
        (highest_level_champion is null or sphere_of_influence is null or maturity_curve is null)
        and account_health_rubric_stakeholder_alignment_driver_success_plan_established
        and account_health_rubric_stakeholder_alignment_driver_sucess_plan_updated_in_last_12_months
        and (maturity_curve is null or account_health_rubric_stakeholder_alignment_driver_maturity_curve = 'Good')
        and (sphere_of_influence is null or sphere_of_influence = '4+ Champions')
        and (highest_level_champion is null or account_health_rubric_stakeholder_alignment_driver_highest_level_champion = 'Good')
    then 'Good'

--poor scenarios
    when
        not account_health_rubric_stakeholder_alignment_driver_success_plan_established
        and not account_health_rubric_stakeholder_alignment_driver_executive_sponsor_engagement_in_last_6_months
        and not account_health_rubric_stakeholder_alignment_driver_qbr_in_last_6_months
        and highest_level_champion in ('VP HR','Manager/Director HR')
    then 'Poor'

    when
        sphere_of_influence = '1-3 Champions'
        and maturity_curve = '1 - Learning Programs'
        and not account_health_rubric_stakeholder_alignment_driver_qbr_in_last_6_months
        and highest_level_champion in ('VP HR','Manager/Director HR')
    then 'Poor'

    when
        (maturity_curve is null or highest_level_champion is null)
        and sphere_of_influence = '1-3 Champions'
        and not account_health_rubric_stakeholder_alignment_driver_success_plan_established
    then 'Poor' else 'Okay' end as account_health_rubric_stakeholder_alignment_overall_score

from accounts_history
inner join eligible_accounts
    on eligible_accounts.sfdc_account_id = accounts_history.sfdc_account_id
