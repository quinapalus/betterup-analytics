{{
  config(
    tags=["eu"]
  )
}}

with coach_profiles as (
  select * from {{ ref('int_coach__coach_profiles') }}
),

billable_events as (
  select * from {{ ref('dbt_events__completed_billable_event') }}
),

versions as (
    select * from {{ ref('int_coach__versions_rollup') }}
),

billable_event_measures as (
  select
    coach_id,
    count(distinct
      case
        when be.attributes:billing_event_type in ('completed_sessions','completed_group_sessions')
        then be.associated_record_id end) as lifetime_completed_sessions,

    count(distinct
      case
        when be.attributes:is_primary_b2b_flag and be.attributes:coaching_cloud = 'professional'
             and be.attributes:billing_event_type in ('completed_sessions')
        then be.associated_record_id end) as lifetime_completed_primary_b2b_sessions,
    sum(
      case
        when be.attributes:billing_event_type in ('completed_sessions','completed_group_sessions')
        then be.attributes:amount_due_usd end) as lifetime_earnings_usd,
    sum(
      case
        when be.attributes:is_primary_b2b_flag and be.attributes:coaching_cloud = 'professional'
             and be.attributes:billing_event_type in ('completed_sessions')
        then be.attributes:amount_due_usd end) as lifetime_primary_b2b_earnings_usd,
    sum(
      case
        when be.attributes:billing_event_type in ('completed_sessions','completed_group_sessions')
        then be.attributes:billable_event_hours end) as lifetime_hours,
    sum(
      case
        when be.attributes:is_primary_b2b_flag and be.attributes:coaching_cloud = 'professional'
             and be.attributes:billing_event_type in ('completed_sessions')
        then be.attributes:billable_event_hours end) as lifetime_primary_b2b_hours

  from billable_events as be
  group by 1)

  select 
    cp.*,
    coalesce(bem.lifetime_completed_sessions,0) as lifetime_completed_sessions,
    coalesce(bem.lifetime_completed_primary_b2b_sessions,0) as lifetime_completed_primary_b2b_sessions,
    coalesce(bem.lifetime_earnings_usd,0) as lifetime_earnings_usd,
    coalesce(bem.lifetime_primary_b2b_earnings_usd,0) as lifetime_primary_b2b_earnings_usd,
    coalesce(bem.lifetime_hours,0) as lifetime_hours,
    coalesce(bem.lifetime_primary_b2b_hours,0) as lifetime_primary_b2b_hours,
    v.first_staffable_at,
    v.last_staffable_at

from coach_profiles cp
left join billable_event_measures bem 
    on bem.coach_id = cp.coach_id
left join versions as v
    on cp.coach_profile_uuid = v.coach_profile_uuid