{{
  config(
    tags=["eu"] )
}}

with tracks as (
  select * from {{ ref('stg_app__tracks') }}
),
organizations as (
  select * from {{ref('stg_app__organizations')}}
),
curriculum_solutions as (
  select * from {{ ref('stg_curriculum__solutions')}}
),
salesforce_programs as (
  select * from {{ ref('stg_sfdc__programs')}}
),
bu_deployment_types as (
  select * from {{ ref('stg_gsheets_deployments__bu_deployment_types')}}
),
track_configurations as (
  select * from {{ ref('int_app__track_configurations') }}
),
track_metrics as (
  select * from {{ref('int_track_metrics')}}
),
track_roles as (
  select * from {{ref('int_track_roles')}}
),
active_storage_attachments as (
  select * from {{ref('stg_app__active_storage_attachments')}}
),
external_resources as (
  select
    record_id as track_id,
    count(*) as external_resource_count
  from active_storage_attachments
  where record_type = 'Track'
    and name = 'external_resources'
  group by track_id
),
final as (

  select
    --primary key
    t.track_id,

    --foreign keys
    sp.sfdc_program_id,
    o.sfdc_account_id,
    t.sfdc_opportunity_id,
    case 
      -- only pass through admin panel `sfdc_opportunity_id` for Sales Trials
      when (d.is_sales_trial is not null) then t.sfdc_opportunity_id else null
    end as sfdc_prospective_opportunity_id,
    c.default_resource_list_id,
    t.organization_id,
    t.contract_id,
    c.stripe_pricing_plan_id,
    t.solution_uuid,
    t.salesforce_program_identifier,

    --logical data
    t.name,
    t.program_name as program_name,
    sp.sfdc_program_name,
    c.overview,
    t.minutes_limit,
    t.members_limit,
    t.length_days,
    c.reflection_point_interval_days,
    c.reflection_point_interval_appointments,
    c.num_reflection_points,
    coalesce(c.deployment_type, t.deployment_type) as deployment_type,
    d.deployment_name,
    case
        when coalesce(c.deployment_type, t.deployment_type) in ('standard', 'standard_and_coaching_circles', 'standard_smb', 'coaching_circles_only', 'care') then 'B2B / Gov Paid Contract'
        when coalesce(c.deployment_type, t.deployment_type) in ('bu_employee', 'bu_friends_and_family') then 'BetterUp'
        when coalesce(c.deployment_type, t.deployment_type) in ('marketing_existing_client','marketing_other','marketing_prospective_client') then 'Marketing'
        when coalesce(c.deployment_type, t.deployment_type) in ('pilot','pilot_smb','trial','trial_care','trial_smb') then 'Pilot / Trial'
        when coalesce(c.deployment_type, t.deployment_type) in ('qa','research_and_development','onboarding_coaches') then 'Test'
        when coalesce(c.deployment_type, t.deployment_type) = 'direct_pay' then 'D2C'
        else coalesce(c.deployment_type, t.deployment_type)
    end as deployment_group,
    t.estimated_seat_count,
    c.member_orientation,
    t.use_cases,
    c.customer_goals,
    c.key_satisfaction_driver,
    c.wpm_behavior_goals,
    t.aggregate_hours_limit,
    c.deployment_cadence,
    c.staffing_member_levels,
    c.program_briefing_duration_minutes,
    c.staffing_risk_levels,
    c.staffing_tiers,
    c.staffing_industries,
    c.client_limit,
    c.staffing_languages,
    {{string_to_array()}}(c.cached_tag_list, ', ') as staffing_qualifications,
    {{string_to_array()}}(c.cached_account_tag_list, ', ') as account_qualifications,
    {{string_to_array()}}(c.cached_certification_tag_list,', ' ) as certification_qualifications,
    {{string_to_array()}}(c.cached_focus_tag_list, ', ') as focus_qualifications,
    {{string_to_array()}}(c.cached_postgrad_tag_list, ', ') as postgrad_qualifications,
    {{string_to_array()}}(c.cached_product_tag_list, ', ') as product_qualifications,
    {{string_to_array()}}(c.cached_professional_tag_list, ', ') as professional_qualifications,
    {{string_to_array()}}(c.cached_segment_tag_list, ', ') as segment_qualifications,
    c.internal_notes,
    cs.name as solution,
    t.primary_coaching_months_limit,
    tm.open_track_assignment_count,
    tm.engaged_member_count,
    em.name as engagement_manager,
    rm.name as relationship_manager,  
    o.name AS organization_name,
    er.external_resource_count,
    d.accounting_category,
    tm.admin_panel_url,
    tm.partner_panel_url,
    t.coaching_cloud,

    --booleans
    (c.launches_on_from_config is null or c.launches_on_from_config <= current_date)
      and (t.ends_on is null or t.ends_on >= current_date) as is_active_track,
    t.ends_on < current_date as is_past_track,
    c.launches_on_from_config > current_date as is_future_track,
    c.stripe_pricing_plan_id <> '' as is_direct_pay,
    coalesce(d.is_external,FALSE) as is_external,
    coalesce(d.is_revenue_generating,FALSE) as is_revenue_generating,
    coalesce(t.restricted,FALSE) as is_restricted,
    coalesce(c.includes_behavioral_assessments,FALSE) as includes_behavioral_assessments,
    coalesce(c.manager_feedback_enabled,FALSE) as manager_feedback_enabled,
    coalesce(c.whole_person360_enabled,FALSE) as whole_person360_enabled,
    coalesce(c.whole_person180_enabled,FALSE) as whole_person180_enabled,
    coalesce(t.coaching_primary_enabled,FALSE) as coaching_primary_enabled,
    coalesce(t.coaching_on_demand_enabled,FALSE) as coaching_on_demand_enabled,
    coalesce(t.coaching_extended_network_enabled,FALSE) as coaching_extended_network_enabled,
    coalesce(c.whole_person_model_2,FALSE) as whole_person_model_2,
    coalesce(c.downloadable,FALSE) as downloadable,
    coalesce(c.program_briefing_automatic_payment,FALSE) as program_briefing_automatic_payment,
    coalesce(t.is_videochat_recording_enabled,FALSE) as is_videochat_recording_enabled,
    t.primary_coaching_months_limit is not null as has_primary_coaching_months_limit,
    coalesce(tm.open_track_assignment_count > 0, FALSE) is_active,
    case when c.deployment_type in ('direct_pay','private_pay') then TRUE 
      else FALSE
      end as is_direct,
    t.is_deleted as is_deleted,
  
    --timestamps
    t.created_at,
    t.updated_at,
    t.ends_on,
    {{ load_timestamp('launches_on_from_config','launches_on') }},
    date_trunc('DAY', tm.start_date) as start_date,
    date_trunc('DAY', tm.end_date) as end_date
 
  from tracks as t
  left join track_metrics as tm
    on t.track_id = tm.track_id
  left join track_configurations as c
    on t.track_id = c.track_id
  left join curriculum_solutions as cs 
    on t.solution_uuid = cs.solution_uuid
  left join bu_deployment_types as d 
    on c.deployment_type = d.deployment_type
  left join salesforce_programs as sp 
    on sp.sfdc_program_id = t.salesforce_program_identifier
  left join track_roles as em
    on t.track_id = em.track_id 
    and em.role = 'engagement_manager'
  left join track_roles as rm
    on t.track_id = rm.track_id
    and rm.role = 'relationship_manager'
  left join organizations as o
    on t.organization_id = o.organization_id
  left join external_resources as er
    on t.track_id = er.track_id

)
select * from final