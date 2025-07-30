--documentation on what this model is doing 
--https://betterup.atlassian.net/wiki/spaces/AN/pages/3147989058/Final+Account+Health+Score+Calculation

--setting variables
{% set count_missing_rubrics_threshhold = 2 %}

with eligible_accounts as (

    select * from {{ ref('int_account_health_eligible_accounts') }}
    --all accounts that meet criteria from here
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3141402630/Member+Populations+and+Timing+of+Metrics+in+Account+Health+2.0+Model#Excluded-Accounts-Cheatsheet

),

accounts as (
    select * from {{ ref('int_sfdc__accounts_snapshot') }}
    where is_current_version and not is_deleted --only want the most recent snapshots
),

rubric_external_factors as (
    select * from {{ ref('int_account_health_rubric_external_factors') }}
),

rubric_stakeholder_alignment as (
    select * from {{ ref('int_account_health_rubric_stakeholder_alignment') }}
),

rubric_adoption as (
    select * from {{ ref('int_account_health_rubric_adoption') }}
),

rubric_engagement as (
    select * from {{ ref('int_account_health_rubric_engagement') }}
),

rubric_nps as (
    select * from {{ ref('int_account_health_rubric_nps') }}
),

rubric_behavioral as (
    select * from {{ ref('int_account_health_rubric_behavioral') }}
),

eligible_accounts_with_rubrics as (

    select
        
        eligible_accounts.sfdc_account_id,
        eligible_accounts.is_eligible_for_account_health_scoring,

        --external factor columns
        rubric_external_factors.account_health_rubric_external_factors_driver_industry,
        rubric_external_factors.account_health_rubric_external_factors_driver_annual_revenue,
        rubric_external_factors.account_health_rubric_external_factors_overall_score,
        {{ convert_account_health_categorical_score_to_numeric('account_health_rubric_external_factors_overall_score') }} as account_health_rubric_external_factors_numerical_overall_score,

        --stakeholder alignment columns
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_maturity_curve,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_highest_level_champion,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_sphere_of_influence,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_qbr_in_last_6_months,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_executive_sponsor_engagement_in_last_6_months,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_success_plan_established,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_driver_sucess_plan_updated_in_last_12_months,
        rubric_stakeholder_alignment.account_health_rubric_stakeholder_alignment_overall_score,
        {{ convert_account_health_categorical_score_to_numeric('account_health_rubric_stakeholder_alignment_overall_score') }} as account_health_rubric_stakeholder_alignment_numerical_overall_score,

        --adoption columns
        rubric_adoption.invited_members_past_30_days as invited_members_past_30_days_adoption_rubric,
        rubric_adoption.activated_members_past_30_days_from_invited as activated_members_past_30_days_from_invited_adoption_rubric,
        rubric_adoption.members_past_30_days as members_past_30_days_adoption_rubric,
        rubric_adoption.members_with_1_session_past_30_days as members_with_1_session_past_30_days_adoption_rubric,
        rubric_adoption.percent_invited_members_activated_past_30_days as percent_invited_members_activated_past_30_days_adoption_rubric,
        rubric_adoption.percent_members_with_1_session_past_30_days as percent_members_with_1_session_past_30_days_adoption_rubric,
        rubric_adoption.average_of_metrics as average_of_metrics_adoption_rubric,
        rubric_adoption.account_health_rubric_adoption_driver_percent_completed_first_session,
        rubric_adoption.account_health_rubric_adoption_driver_percent_activated,
        rubric_adoption.account_health_rubric_adoption_overall_score,
        {{ convert_account_health_categorical_score_to_numeric('account_health_rubric_adoption_overall_score') }} as account_health_rubric_adoption_numerical_overall_score,

        --engagement columns
        rubric_engagement.members_past_30_days as members_past_30_days_engagement_rubric,
        rubric_engagement.members_past_120_days as members_past_120_days_engagement_rubric,
        rubric_engagement.members_with_1_session_past_30_days as members_with_1_session_past_30_days_engagement_rubric,
        rubric_engagement.members_with_1_session_past_120_days as members_with_1_session_past_120_days_engagement_rubric,
        rubric_engagement.total_billable_sessions as total_billable_sessions_engagement_rubric,
        rubric_engagement.members_with_1_reflection_point_past_120_days as members_with_1_reflection_point_past_120_days_engagement_rubric,
        rubric_engagement.average_of_total_months_in_tenure_since_activation as average_of_total_months_in_tenure_since_activation_engagement_rubric,
        rubric_engagement.average_sessions_per_member_per_month as average_sessions_per_member_per_month_engagement_rubric,
        rubric_engagement.percent_of_members_with_1_reflection_point as percent_of_members_with_1_reflection_point_engagement_rubric,
        rubric_engagement.account_health_rubric_adoption_driver_average_sessions_per_member_per_month,
        rubric_engagement.account_health_rubric_adoption_driver_reflection_points,
        rubric_engagement.account_health_rubric_engagement_overall_score,
        {{ convert_account_health_categorical_score_to_numeric('account_health_rubric_engagement_overall_score') }} as account_health_rubric_engagement_numerical_overall_score,

        --nps columns
        rubric_nps.count_activated_members as count_activated_members_nps_rubric,
        rubric_nps.sum_nps_promoters as sum_nps_promoters_nps_rubric,
        rubric_nps.sum_nps_detractors as sum_nps_detractors_nps_rubric,
        rubric_nps.count_item_responses as count_item_responses_nps_rubric,
        rubric_nps.member_net_promoter_score as member_net_promoter_score_nps_rubric,
        rubric_nps.account_health_rubric_nps_driver_member_nps,
        rubric_nps.account_health_rubric_nps_driver_partner_rps,
        rubric_nps.account_health_rubric_nps_overall_score,
        {{ convert_account_health_categorical_score_to_numeric('account_health_rubric_nps_overall_score') }} as account_health_rubric_sentiment_numerical_overall_score,

        --behaviorial columns
        rubric_behavioral.mindset_average_percent_growth_from_reference as mindset_average_percent_growth_from_reference_behavioral_rubric,
        rubric_behavioral.thriving_average_percent_growth_from_reference as thriving_average_percent_growth_from_reference_behavioral_rubric,
        rubric_behavioral.inspiring_average_percent_growth_from_reference as inspiring_average_percent_growth_from_reference_behavioral_rubric,
        rubric_behavioral.outcome_average_percent_growth_from_reference as outcome_average_percent_growth_from_reference_behavioral_rubric,
        rubric_behavioral.overall_average_percent_growth_from_reference as overall_average_percent_growth_from_reference_behavioral_rubric,
        rubric_behavioral.account_health_rubric_behavorial_overall_score,
         {{ convert_account_health_categorical_score_to_numeric('account_health_rubric_behavorial_overall_score') }} as account_health_rubric_behavioral_numerical_overall_score
       
    from eligible_accounts
    left join rubric_external_factors
        on rubric_external_factors.sfdc_account_id = eligible_accounts.sfdc_account_id
    left join rubric_stakeholder_alignment
        on rubric_stakeholder_alignment.sfdc_account_id = eligible_accounts.sfdc_account_id
    left join rubric_adoption
        on rubric_adoption.sfdc_account_id = eligible_accounts.sfdc_account_id
    left join rubric_engagement
        on rubric_engagement.sfdc_account_id = eligible_accounts.sfdc_account_id
    left join rubric_nps
        on rubric_nps.sfdc_account_id = eligible_accounts.sfdc_account_id
    left join rubric_behavioral
        on rubric_behavioral.sfdc_account_id = eligible_accounts.sfdc_account_id

),

eligible_accounts_with_data_availability as (

    select
        *,
        --these availability columns are checking if a given account has a score for reach rubric. 
        --these are used to join to the weight redistribution mapping table 
       iff(account_health_rubric_stakeholder_alignment_overall_score is null
            or account_health_rubric_stakeholder_alignment_overall_score = 'Not Enough Data', 1, 0) as stakeholder_alignment_is_missing,
       iff(account_health_rubric_external_factors_overall_score is null
            or account_health_rubric_external_factors_overall_score = 'Not Enough Data', 1, 0) as external_factors_is_missing,
       iff(account_health_rubric_adoption_overall_score is null
            or account_health_rubric_adoption_overall_score = 'Not Enough Data',1, 0) as adoption_is_missing,
       iff(account_health_rubric_engagement_overall_score is null
            or account_health_rubric_engagement_overall_score = 'Not Enough Data',1,0) as engagement_is_missing,
       iff(account_health_rubric_nps_overall_score is null
            or account_health_rubric_nps_overall_score = 'Not Enough Data',1,0) as sentiment_is_missing,
       iff(account_health_rubric_behavorial_overall_score is null
            or account_health_rubric_behavorial_overall_score = 'Not Enough Data',1,0) as behavioral_is_missing,

        --count of missing rubrics for each account. If two or more rubrics are missing then the account is not scored
         external_factors_is_missing + stakeholder_alignment_is_missing + adoption_is_missing
         + engagement_is_missing + sentiment_is_missing + behavioral_is_missing as count_of_missing_rubrics,

         count_of_missing_rubrics = 0 as is_not_missing_any_rubrics,

        --check if nps and behavioral are both missing. if both of these are missing special weighting rules apply
        case when 
                sentiment_is_missing and behavioral_is_missing 
             then true else false end as is_missing_sentiment_and_behavioral
        
    from eligible_accounts_with_rubrics as accounts

),

eligible_accounts_with_numeric_rubric_scores as (

    select
        accounts.*,

        --reweighting logic for external factors
        case
            when is_not_missing_any_rubrics
                then 0.1
            when is_missing_sentiment_and_behavioral and count_of_missing_rubrics = {{count_missing_rubrics_threshhold}}
                then 0.1625
            when stakeholder_alignment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.18
            when adoption_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.125
            when engagement_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.125
            when sentiment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.13
            when behavioral_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.12
            else 0 end as external_factors_weight,

        --reweighting logic for stakeholder alignment
        case
            when is_not_missing_any_rubrics
                then 0.4
            when is_missing_sentiment_and_behavioral and count_of_missing_rubrics = {{count_missing_rubrics_threshhold}}
                then 0.4625
            when external_factors_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.42
            when adoption_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.425
            when engagement_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.425
            when sentiment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.43
            when behavioral_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.42
            else 0 end as stakeholder_alignment_weight,

        --reweighting logic for adoption
        case
            when is_not_missing_any_rubrics
                then 0.125
            when is_missing_sentiment_and_behavioral and count_of_missing_rubrics = {{count_missing_rubrics_threshhold}}
                then 0.1875
            when external_factors_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.145
            when stakeholder_alignment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.205
            when engagement_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.15
            when sentiment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.155
            when behavioral_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.145
            else 0 end as adoption_weight,

        --reweighting logic for engagement
        case
            when is_not_missing_any_rubrics
                then 0.125
            when is_missing_sentiment_and_behavioral and count_of_missing_rubrics = {{count_missing_rubrics_threshhold}}
                then 0.1875
            when external_factors_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.145
            when stakeholder_alignment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.205
            when adoption_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.15
            when sentiment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.155
            when behavioral_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.17
            else 0 end as engagement_weight,

        --reweighting logic for sentiment
        case
            when is_not_missing_any_rubrics
                then 0.15
            when external_factors_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.17
            when stakeholder_alignment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.23
            when adoption_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.175
            when engagement_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.175
            when behavioral_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.17
            else 0 end as sentiment_weight,

        --reweighting logic for behavioral
        case
            when is_not_missing_any_rubrics
                then 0.1
            when external_factors_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.12
            when stakeholder_alignment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.18
            when adoption_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.125
            when engagement_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.125
            when sentiment_is_missing and count_of_missing_rubrics < {{count_missing_rubrics_threshhold}}
                then 0.13
            else 0 end as behavioral_weight

    from eligible_accounts_with_data_availability accounts
)

select 
    scored_accounts.*,

    --multiplying the numeric scores with the weights
    external_factors_weight * account_health_rubric_external_factors_numerical_overall_score as external_factors_numerical_score,
    stakeholder_alignment_weight * account_health_rubric_stakeholder_alignment_numerical_overall_score as stakeholder_alignment_numerical_score,
    adoption_weight * account_health_rubric_adoption_numerical_overall_score as adoption_numerical_score,
    engagement_weight * account_health_rubric_engagement_numerical_overall_score as engagement_numerical_score,
    sentiment_weight * account_health_rubric_sentiment_numerical_overall_score as sentiment_numerical_score,
    behavioral_weight * account_health_rubric_behavioral_numerical_overall_score as behavioral_numerical_score,

    --summing together all the weighted numerical scores
    external_factors_numerical_score + stakeholder_alignment_numerical_score + adoption_numerical_score + engagement_numerical_score
    + sentiment_numerical_score + behavioral_numerical_score as overall_weighted_average_score,

    --logic to generate the overall categorical score
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3147989058/Final+Account+Health+Score+Calculation
    case
        when overall_weighted_average_score = 0
            then 'Not Enough Data'
        when count_of_missing_rubrics > 1 and not is_missing_sentiment_and_behavioral
            then 'Not Enough Data'
        when is_missing_sentiment_and_behavioral and count_of_missing_rubrics >= 3
            then 'Not Enough Data'
        when overall_weighted_average_score >= 7.15
            then 'Good'
        when overall_weighted_average_score between 4.7 and 7.15
            then 'Okay'
        when overall_weighted_average_score <= 4.7
            then 'Poor' else null end as overall_account_health_categorical_score,

    --logic to generate the overall categorical score. 
    --This is the same as the overall_account_health_categorical_score but with additional logic to allow for CSM overrides
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3147989058/Final+Account+Health+Score+Calculation
    case
        when accounts.csm_health_rating = 'At Risk'
            then 'Poor'
        when accounts.csm_health_rating = 'Somewhat at Risk' and overall_weighted_average_score < 6.6
            then 'Poor'
        when overall_weighted_average_score = 0
            then 'Not Enough Data'
        when count_of_missing_rubrics > 1 and not is_missing_sentiment_and_behavioral
            then 'Not Enough Data'
        when is_missing_sentiment_and_behavioral and count_of_missing_rubrics >= 3
            then 'Not Enough Data'
        when overall_weighted_average_score >= 7.15
            then 'Good'
        when overall_weighted_average_score between 4.7 and 7.15
            then 'Okay'
        when overall_weighted_average_score < 4.7
            then 'Poor' else 'Okay' end as overall_account_health_categorical_score_csm_risk_flag,

    case
        when accounts.csm_health_rating = 'At Risk'
            then 'CSM Risk Override'
        when accounts.csm_health_rating = 'Somewhat at Risk' and overall_weighted_average_score < 6.6
            then 'CSM Risk Override'
        else 'Model Calculation' end as overall_account_health_scoring_method

from eligible_accounts_with_numeric_rubric_scores as scored_accounts
left join accounts
    on scored_accounts.sfdc_account_id = accounts.sfdc_account_id
