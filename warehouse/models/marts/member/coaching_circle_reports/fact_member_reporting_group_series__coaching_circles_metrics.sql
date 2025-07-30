{{ config(
    tags=['coaching_circles_metrics','eu'],
) }}


with reporting_group_assignments as (

    select * from {{ref('dim_reporting_group_assignments')}}
    where reporting_group_id in (
          select reporting_group_id from  {{ref('dim_reporting_groups')}}
          where product_type in ('coaching_circles')
      )

),

reporting_group_engagement_metrics as (

    select * from {{ref('fact_reporting_group_engagement_metrics')}}

),

invited_events as (

    select * from {{ref('fact_member_events')}}
    where event_action_and_object in ('invited coaching_circles_product', 'invited workshops_product')

),

series_invited_events as (

    select *,
        attributes:track_name::varchar as track_name,
        attributes:track_assignment_ended_at::timestamp_ntz as track_assignment_ended_at,
        attributes:group_coaching_series_id::int as series_id,
        attributes:registration_start::timestamp_ntz as registration_starts_at,
        attributes:registration_end::timestamp_ntz  as registration_ends_at
    from {{ref('fact_member_events')}}
    where event_action_and_object in ('invited coaching_circle_series', 'invited workshop_series')

),

registered_events as (

    select *,
        attributes:group_coaching_series_id::int as series_id,
        attributes:"group_coaching_cohort_id"::int as cohort_id
    from {{ref('fact_member_events')}}
    where event_action_and_object in ('registered coaching_circle', 'registered workshop')

),

activated_events as (

    select member_id, min(event_at) as event_at
    from {{ref('fact_member_events')}}
    where event_action_and_object in ('activated coaching_circles_user', 'activated workshops_user')
    group by member_id

),

onboarded_assessment_events as (

  select
      member_id,
      min(event_at) as event_at
  from {{ref('fact_member_events')}}
  where event_name = 'submitted assessment'
      and attributes:"assessment_type"::varchar in (
      'Assessments::OnboardingAssessment',
      'Assessments::PrimaryCoachingModalitySetupAssessment',
      'Assessments::WholePersonAssessment'
      )
  group by member_id

),

attended_events as (

    select member_id,
         attributes:"group_coaching_series_id"::int as series_id,
         attributes:"group_coaching_cohort_id"::int as cohort_id,
         associated_record_id
    from {{ref('fact_member_events')}}
    where event_action_and_object in ('attended coaching_circle_appointment', 'attended workshop_appointment')

),


registered_events_by_member_series as (

  select
      member_id,
      series_id,
      min(event_at) as event_at
  from registered_events
  group by member_id, series_id

),

registered_events_by_member_series_cohort as (

  select
      member_id,
      series_id,
      cohort_id
  from registered_events
  group by member_id, series_id, cohort_id

)

, final as (

    select 
        --TO-DO: Creating primary key of the table as it exists now, but 
        --it feels like the granularity is incorrect and needs to be fixed.
        {{ dbt_utils.surrogate_key(['ie.member_id', 'rga.reporting_group_id', 'sie.series_id', 'sie.track_name', 'sie.track_assignment_ended_at',
            'ae.associated_record_id', 'ie.associated_record_id', 'rgem.status', 'rgem.ended_at']) }} as primary_key,
        ie.member_id::int as member_id,
        rga.reporting_group_id,
        sie.track_name,
        sie.series_id,
        sie.track_assignment_ended_at,
        rgem.status as activation_status,
        rgem.started_at as circles_access_started_at,
        rgem.ended_at as circles_access_ended_at,
        coalesce(ae.associated_record_id::int, ie.associated_record_id::int) as associated_record_id,
        re.event_at as registered_at,
        registration_starts_at,
        registration_ends_at,
        ace.event_at as activated_at,
        oae.event_at as onboarded_at
    from invited_events as ie
    inner join reporting_group_assignments as rga
        on ie.member_id = rga.member_id
        and ie.event_at >= rga.starts_at
        and (rga.ended_at is null or ie.event_at < rga.ended_at)
    left join reporting_group_engagement_metrics as rgem
        on rgem.member_id = rga.member_id 
        and rgem.reporting_group_id = rga.reporting_group_id
    left join series_invited_events as sie
        on ie.member_id = sie.member_id 
        and ie.event_at <= sie.attributes:registration_end::timestamp_ntz
        and replace(ie.event_object, 's_product') = sie.attributes:intervention_type::varchar
    left join registered_events_by_member_series as re
        on ie.member_id = re.member_id 
        and sie.series_id = re.series_id
    left join registered_events_by_member_series_cohort as remsc
        on re.member_id = remsc.member_id 
        and re.series_id = remsc.series_id
    left join activated_events as ace
        on ie.member_id = ace.member_id
    left join onboarded_assessment_events as oae
        on ie.member_id = oae.member_id
    left join attended_events as ae
        on remsc.member_id = ae.member_id 
        and remsc.cohort_id = ae.cohort_id 
        and remsc.series_id = ae.series_id
    --Band-aid solution: fanout should be handled in CTEs or upstream
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

)

select * from final
