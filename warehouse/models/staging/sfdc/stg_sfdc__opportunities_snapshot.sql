with opportunity_snapshot as (

    select * from {{ ref('snapshot_sfdc_opportunities') }}

)

select 
    --ids
    id as sfdc_opportunity_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
    account_id as sfdc_account_id,
    created_by_id as opportunity_created_by_id,
    owner_id as opportunity_owner_id,
    campaign_id as primary_campaign_source_campaign_id,
    opportunity_sc_c as opportunity_sc_id,
    opportunity_sc_secondary_c as opportunity_secondary_sc_id,
    first_meeting_generated_by_c as opportunity_qualified_by_id,
    executive_sponsor_c as executive_sponsor_id,
    record_type_id as sfdc_record_type_id,

    --categorical and text attributes
    name as opportunity_name,
    type as opportunity_type,
    {{ sanitize_opportunity_stage('stage_name') }} as opportunity_stage,
    stage_name as opportunity_stage_raw,
    sales_stage_c as sales_stage,
    reporting_stage_c as reporting_stage,
    qualification_source_c as qualification_source,
    forecast_category_c as forecast_category,
    win_reason_c as win_reasons,
    win_story_c as win_story,
    lost_reason_c as lost_reasons,
    lost_reason_detail_c as lost_reason_detail,
    next_steps_c as next_steps,
    primary_solution_c as primary_solution,
    secondary_solution_c as secondary_solution,
    likelihood_to_renew_c as likelihood_to_renew,
    likelihood_to_renew_reasons_c as likelihood_to_renew_reasons,
    proposal_link_c as proposal_link,
    risk_status_c as risk_status,
    risk_reason_c as risk_reasons,
    challenge_solution_and_use_case_c as challenge_solution_and_use_case,
    highest_champion_level_c as highest_champion_level,
    opp_owner_region_c as opportunity_owner_region,
    probability*0.01 as opportunity_probability,
    bu_app_instance_c as bu_app_instance,
    deal_size_c as deal_size,
    deal_size_bucket_c as deal_size_bucket,
    potential_value_size_formula_c as potential_value_size_formula,
    potential_size_bucket_field_c as potential_size_bucket_field,

    --quantities
    amount as opportunity_amount_unconverted,
    plan_renewal_amount_c as plan_renewal_amount_unconverted,
    x1_st_year_acv_c as first_year_acv_amount_unconverted,
    potential_value_c as potential_value_unconverted,
    risk_amount_c as risk_amount_unconverted,
    how_many_champions_c as how_many_champions,
    total_users_c as total_users,

    --boooleans
    post_covid_discussion_c as has_post_covid_discussion,
    catalyst_identified_c as has_catalyst_identified,
    business_case_c as has_business_case,
    budget_confirmed_c as has_budget_confirmed,
    c_level_c as has_c_level,
    multi_program_c as has_multi_program,
    potential_pull_forward_c as has_potential_pull_forward,
    validated_for_booking_c as has_been_validated_for_booking,
    contract_in_process_c as has_contract_in_process,
    close_plan_agreed_c as has_close_plan_agreed,
    upside_potential_c as upside_potential,
    is_won,
    is_closed,
    
    --opportunity qualification checklist booleans
    iff(customer_executive_sponsor_c = 'Yes',1,0) as has_secured_customer_executive_sponsor,
    iff(identified_customer_problem_c = 'Yes',1,0) as has_identified_customer_problem,
    iff(proposed_solution_c = 'Yes',1,0) as has_proposed_solution,
    iff(commercial_proporsal_scope_c = 'Yes',1,0) as has_confirmed_commercial_proposal_scope,
    iff(compelling_close_date_c = 'Yes',1,0) as has_compelling_close_date,
    iff(multithreaded_c = 'Yes',1,0) as has_multiple_dmu_champions,
    iff(bu_executive_sponsor_c = 'Yes',1,0) as has_bu_executive_sponsor,
    iff(business_case_roi_picklist_c = 'Yes',1,0) as has_shared_business_case_roi,
    iff(cfo_finance_approved_roi_c = 'Yes',1,0) as has_cfo_finance_approved_roi,
    iff(joint_engagement_plan_picklist_c = 'Yes',1,0) as has_confirmed_joint_engagement_plan,
    iff(unique_differentiators_c = 'Yes',1,0) as has_unique_competitive_differentiation,
    iff(better_up_selected_as_vendor_c = 'Yes',1,0) as has_selected_betterup_as_vendor,
    iff(contracting_c = 'Yes',1,0) as has_confirmed_contracting_plan,
    iff(funding_budget_c = 'Yes',1,0) as has_confirmed_funding_budget,
    iff(deployment_c = 'Yes',1,0) as has_confirmed_deployment_date,

    --meddpicco fields
    metric_score_c as metric_score,
    economic_buyer_score_c as economic_buyer_score,
    decision_process_score_c as decision_process_score,
    paper_process_score_c as paper_process_score,
    identify_pain_score_c as identify_pain_score,
    champion_score_c as champion_score,
    competition_score_c as competition_score,
    operations_internal_score_c as operations_internal_score,
    meddpicco_score_c as meddpicco_score,
    meddpicco_scoring_completion_c as meddpicco_scoring_completion,

    --dates and timestamps
    {{ environment_varchar_to_timestamp('created_date','created_at') }},
    {{ environment_varchar_to_timestamp('last_modified_date','last_modified_at') }},
    {{ environment_varchar_to_timestamp('dbt_valid_from','valid_from') }},
    {{ environment_varchar_to_timestamp('dbt_valid_to','valid_to') }},
    --close date has some additional logic applied in int_sfdc__opportunities_snapshot
    --re-aliasing here to close_date_raw to avoid confusion
    close_date::date as close_date_raw,
    subscription_start_date_c::date as subscription_start_date,
    subscription_end_date_c::date as subscription_end_date,
    renewal_date_c::date as renewal_date,
    initiated_renewal_planning_survey_c::date as initiated_renewal_planning_survey_date,
    discussed_renewal_plans_c::date as discussed_renewal_plans_date,
    initial_proposal_sent_date_c::date as initial_proposal_sent_date,
    initial_jep_sent_date_c::date as initial_jep_sent_date,
    stg1_date_c::date as stage_1_date,
    cfcr_stg2_date_c::date as stage_2_date,
    cfcr_stg3_date_c::date as stage_3_date,
    cfcr_stg4_date_c::date as stage_4_date,
    cfcr_stg5_date_c::date as stage_5_date,
    cfcr_stg6_date_c::date as stage_6_date,
    fm_date_c::date as fm_scheduled_for_date,
    fiscal_year,
    fiscal_quarter,
    start_date_c,
    start_date_per_agreement_c,
    end_date_per_agreement_c,

    --other
    new_business_deal_score_c,
    renewal_risk_score_c,
    sales_region_c,
    forecast_category_c,
    type_c,
    is_deleted,
    currency_iso_code,
    dbt_valid_to is null as is_current_version,
    row_number() over(
      partition by id
      order by dbt_valid_from
    ) as version,
    
    case when
      row_number() over(
        partition by id,date_trunc('day',valid_from)
        order by dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day

from opportunity_snapshot o
