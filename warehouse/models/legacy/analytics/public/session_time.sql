{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='view'
  )
}}

-- This model combines primary and group coaching sessions into a single select by unioning the two datasets.
-- Once unioned, billable event level data is added in.
-- This data needs to be combined so that primary and group coaching session time can be aggregated.

WITH billable_events AS(
  SELECT * FROM {{ref('app_billable_events')}}
),

appointments AS(
  SELECT * FROM {{ref('stg_app__appointments')}}
),

coach_assignments AS(
  SELECT * FROM {{ref('stg_app__coach_assignments')}}
),

group_coaching_sessions AS(
  SELECT * FROM {{ref('stg_app__group_coaching_sessions')}}
),

group_coaching_appointments AS(
  SELECT * FROM {{ref('int_app__group_coaching_appointments')}}
),

group_coaching_cohorts AS(
  SELECT * FROM {{ref('int_app__group_coaching_cohorts')}}
),

group_coaching_series AS(
  SELECT * FROM {{ref('stg_app__group_coaching_series')}}
),

group_coaching_curriculums AS(
  SELECT * FROM {{ref('stg_app__group_coaching_curriculums')}}
),

billable_events_filtered AS (
  SELECT
    billable_event_id,
    associated_record_id AS session_id,
    associated_record_type,
    event_at,
    to_char(event_at, 'YYYYMMDD') AS date_key,
    event_type,
    coaching_type,
    member_id,
    usage_minutes,
    row_number() over(partition by associated_record_id, event_type order by created_at desc) AS rn
  FROM billable_events AS be
  WHERE (ASSOCIATED_RECORD_TYPE = 'Session' OR ASSOCIATED_RECORD_TYPE = 'GroupCoachingSession')
  AND (EVENT_TYPE = 'completed_sessions' OR EVENT_TYPE = 'completed_group_sessions')
 ),

sessions AS (
  SELECT
    a.appointment_id AS session_id,
    a.member_id AS member_id,
    requested_length,
    CASE WHEN ca.role = 'secondary' THEN 'extended_network'
    ELSE ca.role
    END AS type,
    to_char(starts_at, 'YYYYMMDD') AS date_key,
    'Appointment' AS eventable_type_join,
    session_id AS eventable_id_join,
    'completed' AS verb_join
  FROM appointments AS a
  LEFT JOIN coach_assignments AS ca ON a.coach_assignment_id = ca.coach_assignment_id
),

gc_sessions AS (
  SELECT
    s.group_coaching_session_id AS session_id,
    a.member_id,
    c.session_duration_minutes as requested_length,
    cu.intervention_type AS type,
    to_char(s.starts_at, 'YYYYMMDD') as date_key,
    'GroupCoachingCohort' AS eventable_type_join,
    c.group_coaching_cohort_id AS eventable_id_join,
    'joined' AS verb_join
  FROM group_coaching_sessions AS s
  LEFT JOIN group_coaching_appointments AS a on s.group_coaching_session_id = a.group_coaching_session_id
  LEFT JOIN group_coaching_cohorts AS c ON s.group_coaching_cohort_id = c.group_coaching_cohort_id
  LEFT JOIN group_coaching_series AS sr ON c.group_coaching_series_id = sr.group_coaching_series_id
  LEFT JOIN group_coaching_curriculums AS cu ON sr.group_coaching_curriculum_id = cu.group_coaching_curriculum_id
),

union_sessions AS (
  SELECT * FROM sessions
  UNION ALL SELECT * FROM gc_sessions
), 

final AS (
  
  SELECT
    {{ dbt_utils.surrogate_key(['billable_event_id','union_sessions.member_id'])}} as session_time_id,
    billable_event_id,
    session_id,
    union_sessions.member_id,
    requested_length,
    usage_minutes,
    type,
    date_key,
    eventable_id_join,
    eventable_type_join,
    verb_join
  FROM billable_events_filtered
  LEFT JOIN union_sessions USING(session_id, date_key)
  WHERE rn = 1 -- Filtering for most recent billable event created at
)

select * from final