{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH sessions AS (

  SELECT * FROM {{ source('app', 'sessions') }}

)


SELECT
  id AS session_id,
  {{ load_timestamp('starts_at') }},
  {{ load_timestamp('ends_at') }},
  requested_length,
  user_id AS member_id,
  coach_id,
  {{ load_timestamp('complete_at') }},
  {{ load_timestamp('started_at') }},
  length,
  call_id,
  missed,
  {{ load_timestamp('canceled_at') }},
  canceled_by_user_id,
  recurring,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }},
  creator_id,
  contact_method,
  coach_assignment_id,
  track_assignment_id,
  post_session_member_assessment_id,
  development_topic_id,
  timeslot_id,
  requested_weekly_interval
FROM sessions
