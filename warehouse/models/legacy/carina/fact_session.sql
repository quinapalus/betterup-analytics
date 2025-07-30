WITH session AS (

  SELECT * FROM {{ref('dbt_session')}}

),

session_inferred_call_quality AS (

  SELECT * FROM {{ref('dbt_session_inferred_call_quality')}}

),

session_app_call_quality AS (

  SELECT * FROM {{ref('dbt_session_app_call_quality')}}

),

session_call_quality AS (

  -- union both call quality models

  SELECT
    *
  FROM session_inferred_call_quality
  UNION ALL -- we use UNION ALL for performance
  SELECT
    *
  FROM session_app_call_quality

)


SELECT
  -- we join the call quality information on all other session attributes
  s.session_key,
  s.member_key,
  s.coach_key,
  s.account_key,
  s.deployment_key,
  s.member_deployment_key,
  s.date_key,
  s.app_session_id,
  s.app_scheduled_at,
  s.app_starts_at,
  s.scheduled_length,
  s.is_complete,
  s.session_type,
  s.extended_network_session_type,
  s.development_topic_key,
  s.contact_method,
  s.coach_qualitative_feedback_call_quality,
  s.media_mode,
  s.platform_provider,
  s.member_platform,
  s.member_os_browser,
  s.coach_platform,
  s.coach_os_browser,
  s.coach_reported_completed_on_platform,
  s.coach_reported_completed_on_phone,
  s.coach_reported_problem,
  s.member_connected,
  s.coach_connected,
  s.member_submitted_rating,
  s.platform_duration_minutes,
  s.reported_duration_minutes,
  s.coach_feedback_call_quality,
  s.coach_encountered_call_issue,
  s.member_feedback_call_quality,
  s.member_encountered_call_issue,
  s.coach_feedback_covid_topics,
  s.coach_feedback_covid_session_extent,
  s.member_completed_session_sequence,
  sq.platform,
  sq.has_tech_issues,
  sq.issue_type
FROM session AS s
INNER JOIN session_call_quality AS sq
  ON s.session_key = sq.session_key
