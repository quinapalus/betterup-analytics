WITH completed_sessions AS (

  SELECT * FROM {{ref('dbt_completed_sessions')}}

),

member_calls AS (

  SELECT * FROM {{ref('dbt_member_calls')}}

),

sessions AS (

  SELECT * FROM {{ref('dei_sessions')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  where is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

coach_key AS (

  SELECT * FROM {{ref('dbt_coach_key')}}

)


SELECT
  {{ session_key('cs.session_id', 'cs.member_id', 'cs.coach_id', 'cs.starts_at') }} AS session_key,
  {{ member_key('cs.member_id') }} AS member_key,
  ck.coach_key,
  {{ account_key('tr.organization_id', 'tr.sfdc_account_id') }} AS account_key,
  {{ deployment_key('cs.track_id') }} AS deployment_key,
  {{ member_deployment_key('cs.member_id', 'cs.track_id') }} AS member_deployment_key,
  {{ date_key('cs.starts_at') }} AS date_key,
  cs.session_id AS app_session_id,
  se.scheduled_at AS app_scheduled_at,
  cs.starts_at AS app_starts_at,
  se.requested_length AS scheduled_length,
  true AS is_complete, -- stub while only completed sessions
  {{ sanitize_session_type('se.coach_type') }} AS session_type,
  CASE
    WHEN {{ sanitize_session_type('se.coach_type') }} = 'Extended Network' THEN se.extended_network_session_type
    ELSE 'N/A'
  END AS extended_network_session_type,
  {{ development_topic_key('se.development_topic_id' )}} AS development_topic_key,
  mc.contact_method,
  mc.coach_qualitative_feedback_call_quality,
  mc.media_mode,
  mc.platform_provider,
  mc.member_platform,
  mc.member_os_browser,
  mc.coach_platform,
  mc.coach_os_browser,
  mc.coach_reported_completed_on_platform,
  mc.coach_reported_completed_on_phone,
  mc.coach_reported_problem,
  mc.member_connected,
  mc.member_submitted_rating,
  mc.coach_connected,
  mc.platform_duration_minutes,
  cs.reported_duration_minutes,
  mc.coach_feedback_call_quality,
  mc.coach_encountered_call_issue,
  mc.member_feedback_call_quality,
  mc.member_encountered_call_issue,
  mc.coach_feedback_covid_topics,
  mc.coach_feedback_covid_session_extent,
  cs.member_completed_session_sequence
FROM completed_sessions AS cs
INNER JOIN member_calls AS mc
  ON cs.session_id = mc.session_id
INNER JOIN sessions AS se
  ON cs.session_id = se.session_id
INNER JOIN tracks AS tr
  ON cs.track_id = tr.track_id
INNER JOIN coach_key AS ck
  ON cs.coach_id = ck.app_coach_id
-- filter out dates when we had no information on call quality
WHERE cs.starts_at >= '10/18/2018'
