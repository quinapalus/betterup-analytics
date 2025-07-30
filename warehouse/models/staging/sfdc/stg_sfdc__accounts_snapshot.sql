with account_snapshot as (

    select * from {{ ref('snapshot_sfdc_accounts') }}

)

select 
    --ids
    a.id as sfdc_account_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
    a.parent_id as parent_sfdc_account_id,
    a.owner_id as account_owner_id,
    a.acct_csm_c as account_csm_id,
    a.account_manager_c as account_manager_id,
    a.bdr_on_account_c as account_sdr_id,
    a.account_dm_c  as account_dm_id,
    a.account_sc_c as account_sc_id,
    a.better_up_executive_sponsor_c as betterup_executive_sponsor_id,   
    a.dozisf_zoom_info_id_c as zoominfo_id,
    a.zoom_info_ultimate_parent_id_c as zoominfo_ultimate_parent_id,
    a.ultimate_parent_c as ultimate_parent,
    a.record_type_id as sfdc_record_type_id,

    --categorical and text attributes
    a.name as account_name,
    a.industry,
    zi_primary_industry_c as account_primary_industry_zoominfo,
    a.account_segment_c as account_segment,
    a.account_solutions_c as account_solutions,
    a.type as account_type,
    a.billing_city as billing_city,
    a.billing_country as billing_country,
    a.billing_state as billing_state,
    a.company_size_c as company_size,
    a.pricing_recommendation_c as pricing_reccomendation,
    a.expansion_priority_score_c as expansion_priority_score,
    a.lifecycle_c as account_lifecycle_stage,
    a.highest_level_champion_c as highest_level_champion,
    a.x1_st_session_completed_c as first_session_completed_rate,
    a.sphere_of_influence_c as sphere_of_influence,
    a.strategic_catalyst_c as strategic_catalysts,
    a.d_b_region_c as db_region,
    a.account_tier_c as account_tier,
    a.csm_health_rating_c as csm_health_rating,
    a.h2_pod_segment_c as pod_segment,
    a.h2_levels_of_focus_c as levels_of_focus,
    a.h2_adj_account_segment_c as adjusted_account_segment,
    a.h2_bingo_card_bucket_c as bingo_card_bucket,
    a.x6_sense_account_buying_stage_c as six_sense_account_buying_stage,
    a.x6_sense_account_intent_score_c as six_sense_account_intent_score,
    a.x6_sense_account_profile_fit_c as six_sense_account_profile_fit,
    a.x6_sense_account_profile_score_c as six_sense_account_profile_score,
    a.maturity_curve_c as maturity_curve,
    a.success_plan_link_c as success_plan_link,
    a.care_tier_c as care_tier,
    a.primary_risk_reason_c as primary_risk_reason,
    a.csm_region_c as account_csm_region,
    a.account_owner_region_c as account_owner_region,
    a.better_up_segment_c as betterup_segment,
    
    --zoominfo technology data
    a.zoom_info_crm_software_c as zoominfo_crm_software,
    zoom_info_customer_experience_systems_c as zoominfo_customer_experience_systems,
    zoom_info_customer_feedback_systems_c as zoominfo_customer_feedback_systems,
    zoom_info_email_hosting_c as zoominfo_email_hosting_systems,
    zoom_info_erp_software_c as zoominfo_erp_software,
    zoom_info_file_sharing_systems_c as zoominfo_filesharing_systems,
    zoom_info_hr_software_c as zoominfo_hr_software,
    zoom_info_lms_software_c as zoominfo_lms_software,
    zoom_info_operating_systems_c as zoominfo_operating_systems,
    zoom_info_other_comm_collab_systems_c as zoominfo_other_comm_collab_systems,
    zoom_info_other_hr_system_c as zoominfo_other_hr_systems,
    zoom_info_other_it_systems_c as zoominfo_other_it_systems,
    zoom_info_team_collaboration_c as zoominfo_team_collaboration,
    zoom_info_busines_process_systems_c as zoominfo_business_process_systems,

    --technology data entered by our CSMs and AEs
    human_capital_management_c as human_capital_management_systems,
    customer_relations_management_c as customer_relations_management_systems,
    enterprise_resource_planning_c as enterprise_resource_planning_systems,
    learning_management_system_c as learning_management_systems,
    calendar_c as calendar_systems,

    --quantities
    a.number_of_employees,
    a.previous_quarter_carr_c as previous_quarter_carr_unconverted,
    a.of_programs_c as number_of_programs,
    sum_of_all_nps_scores_c as sum_of_all_nps_scores,
    count_of_all_nps_scores_c as count_of_all_nps_scores,
    (sum_of_all_nps_scores * 1.0) / nullif(count_of_all_nps_scores,0) as most_recent_partner_rps,
    {{ convert_sfdc_number_field_scientific_notation('zoom_info_revenue_c') }} as annual_account_revenue_zoominfo_unconverted,

    --boooleans
    case
      when a.parent_id is null 
      then true else false end as is_top_level_parent_account,
    case
      when care_lighthouse_account_c = 'Yes'
      then true else false end as is_care_lighthouse_account,
  
    --dates and timestamps
    --Airbyte lands salesforce datetime fields as varchar but Segment converts them to timestamps before landing them in raw
    --Below macro converts the varchar to timestamp for the Airbyte timestamps that we need

    --dates
    success_plan_last_updated_c::date as success_plan_last_updated_at,
    --timestamps
    {{ environment_varchar_to_timestamp('created_date','created_at') }},
    {{ environment_varchar_to_timestamp('last_modified_date','last_modified_at') }},
    {{ environment_varchar_to_timestamp('last_qbr_date_c','last_qbr_at') }},
    {{ environment_varchar_to_timestamp('last_executive_sponsor_engagement_c','last_executive_sponsor_engagement_at') }},
    {{ environment_varchar_to_timestamp('dbt_valid_from','valid_from') }},
    {{ environment_varchar_to_timestamp('dbt_valid_to','valid_to') }},
    
    --other
    a.is_deleted,
    a.currency_iso_code,
    a.dbt_valid_to is null as is_current_version,
    
    row_number() over(
      partition by a.id
      order by a.dbt_valid_from
    ) as version,
      
    case when
      row_number() over(
        partition by a.id,date_trunc('day',valid_from)
        order by a.dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day
     
from account_snapshot a
