       {{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH billable_events AS (
    SELECT *, to_char(event_at, 'YYYYMMDD') AS event_date_key FROM {{ref('app_billable_events')}}
),

group_coaching_attendance AS (
    SELECT * FROM {{ref('int_group_coaching_attendance')}}
),

track_assignments AS (
    SELECT * FROM {{ref('stg_app__track_assignments')}}
),

tracks AS (
    SELECT * FROM {{ref('dim_tracks')}}
),

member_platform_calendar AS (
    SELECT * FROM {{ref('member_platform_calendar')}}
),

events_other_than_canceled_group AS
(
    SELECT be.associated_record_id AS session_id,
           be.associated_record_type,
           be.billable_event_id,
           be.event_at,
           be.sent_to_processor_at,
           be.event_date_key,
           be.event_type,
           be.coach_id,
           be.amount_due,
           be.currency_code,
           be.usage_minutes,
           be.units,
           be.response_body,
           be.payment_id,
           gca.group_coaching_cohort_id,
           gca.intervention_type,
           gca.group_coaching_series_id,
           gca.session_date,
           gca.session_order,
           coalesce(be.member_id, gca.member_id) AS member_id,
           be.track_id,
           be.track_assignment_id,
           be.product_subscription_assignment_id,
           coalesce(gca.member_registered,IFF(event_type IN ('completed_sessions','late_reschedules' , 'late_cancellation' , 'missed_session'),'Yes','No')) AS member_registered,
           coalesce(gca.member_attempted_to_join,IFF(event_type = 'completed_sessions','Yes','No')) AS member_attempted_to_join, -- will be null for 1:1 sessions - so make it Yes for completed sessions
           gca.max_registrants,
           gca.min_registrants
    FROM billable_events AS be
    LEFT OUTER JOIN group_coaching_attendance AS gca
    ON ((be.event_type = 'completed_group_sessions'
         AND be.associated_record_type = 'GroupCoachingSession'
         AND be.associated_record_id = gca.group_coaching_session_id
         AND gca.status = 'completed'
         ))
    WHERE be.event_type <> 'group_coaching_cohort_canceled'
),

canceled_cohorts AS
(
    SELECT be.associated_record_id AS session_id,
           be.associated_record_type,
           be.billable_event_id,
           be.event_at,
           be.sent_to_processor_at,
           be.event_date_key,
           be.event_type,
           be.coach_id,
           be.amount_due,
           be.currency_code,
           be.usage_minutes,
           be.units,
           be.response_body,
           be.payment_id,
           gca.group_coaching_cohort_id,
           gca.intervention_type,
           gca.group_coaching_series_id,
           gca.session_date,
           gca.session_order,
           coalesce(be.member_id, gca.member_id) AS member_id,
           be.track_id,
           be.track_assignment_id,
           be.product_subscription_assignment_id,
           gca.member_registered,
           gca.member_attempted_to_join,
           gca.max_registrants,
           gca.min_registrants
    FROM billable_events AS be
    LEFT OUTER JOIN group_coaching_attendance AS gca
    ON (be.associated_record_id = gca.group_coaching_cohort_id
         AND gca.status = 'canceled'
         AND gca.session_order = 1 -- we pay only for the first session of a canceled cohort
         )
    WHERE be.event_type = 'group_coaching_cohort_canceled'
      AND be.associated_record_type = 'GroupCoachingCohort'
 ),

all_events AS
( --Snowflake is crazy fast with unions
  -- we are combining mutually exclusive datasets. so union all is good.
      SELECT * FROM events_other_than_canceled_group
       UNION ALL
      SELECT * FROM canceled_cohorts
 ),

final AS (

    SELECT DISTINCT 
        ae.session_id,
        ae.associated_record_type,
        ae.billable_event_id,
        convert_timezone('UTC','America/Los_Angeles', ae.event_at)::date AS event_date,
        CASE WHEN ae.event_type IN ('training_stipend', 'training_session', 'launch_call', 'incentive', 'miscellaneous', 'event_adjustments') THEN convert_timezone('UTC','America/Los_Angeles',DATEADD('month', -1,ae.sent_to_processor_at))::date ELSE convert_timezone('UTC','America/Los_Angeles',ae.sent_to_processor_at)::date END AS event_reported_date,
        convert_timezone('UTC','America/Los_Angeles',ae.sent_to_processor_at)::date AS sent_to_processor_date,
        ae.event_type,
        ae.coach_id,
        ae.amount_due,
        ae.currency_code,
        ae.usage_minutes,
        ae.units,
        ae.response_body,
        ae.payment_id,
        ae.group_coaching_cohort_id,
        ae.intervention_type,
        ae.group_coaching_series_id,
        ae.session_date,
        ae.session_order,
        ae.member_id,
        coalesce(ae.track_assignment_id, mpc.track_assignment_id) AS track_assignment_id,
        t.track_id AS track_id,
        t.organization_id AS organization_id,
        coalesce(ae.product_subscription_assignment_id, mpc.product_subscription_assignment_id) AS product_subscription_assignment_id,
        ae.member_registered,
        ae.member_attempted_to_join,
        ae.max_registrants,
        ae.min_registrants,
        -- some numbers that we need to calculate cost attributed to a specific member for group sessions
        IFF(event_type IN ('completed_group_sessions','group_coaching_cohort_canceled'),COUNT(DISTINCT ae.member_id) OVER (PARTITION BY ae.group_coaching_cohort_id,ae.session_id),1) AS registered_members_per_cohort,
        IFF(event_type IN ('completed_group_sessions','group_coaching_cohort_canceled'),COUNT_IF(ae.member_attempted_to_join IS NOT NULL) OVER (PARTITION BY ae.group_coaching_cohort_id, ae.session_id),1) AS attending_members_per_session,
        IFF(event_type IN ('completed_group_sessions','group_coaching_cohort_canceled'),COUNT(DISTINCT t.track_id) OVER (PARTITION BY ae.group_coaching_cohort_id,ae.session_id),1) AS unique_tracks_per_session,
        IFF(event_type IN ('completed_group_sessions','group_coaching_cohort_canceled'),COUNT(DISTINCT t.organization_id) OVER (PARTITION BY ae.group_coaching_cohort_id,ae.session_id),1) AS unique_orgs_per_session
    FROM all_events AS ae
    LEFT OUTER JOIN member_platform_calendar AS mpc
    ON ae.event_date_key = mpc.date_key AND ae.member_id = mpc.member_id
    LEFT OUTER JOIN tracks AS t
    ON t.track_id = coalesce(ae.track_id, mpc.track_id)

)

SELECT 
    {{ dbt_utils.surrogate_key(['billable_event_id', 'member_id', 'track_id', 'organization_id', 'product_subscription_assignment_id']) }} as primary_key,
    *
FROM final
