WITH calls AS (

  SELECT * FROM {{ref('stg_app__calls')}}

),

sessions AS (

  SELECT * FROM {{ref('stg_app__sessions')}}

),

coach_post_session_assessments AS (

  SELECT * FROM {{ref('dbt_coach_post_session_assessments')}}

),

member_post_session_assessments AS (

  SELECT * FROM {{ref('dbt_member_post_session_assessments')}}

)


SELECT
  c.call_id,
  s.session_id,
  c.member_id,
  c.coach_id,
  s.contact_method,
  c.issues:other::VARCHAR AS coach_qualitative_feedback_call_quality,
  c.media_mode,
  c.platform_provider,
  c.member_client_info:platform::VARCHAR AS member_platform,
  c.member_client_info:os::text || ' ' || c.member_client_info:browser::text AS member_os_browser,
  c.coach_client_info:platform::VARCHAR AS coach_platform,
  c.coach_client_info:os::text || ' ' || c.coach_client_info:browser::text AS coach_os_browser,
  c.completed_on_platform AS coach_reported_completed_on_platform,
  c.completed_on_phone AS coach_reported_completed_on_phone,
  c.coach_reported_problem,
  c.member_connected_at,
  c.member_connected_at IS NOT NULL AS member_connected,
  CASE
    -- only set true/false if session happened after release of updated
    -- post-session assessment on Mar 8, 2019
    WHEN s.starts_at > '03/08/2019' THEN ma.session_id IS NOT NULL
  END AS member_submitted_rating,
  c.coach_connected_at,
  c.coach_connected_at IS NOT NULL AS coach_connected,
  -- calculate the duration of call only when both parties connected
  CASE
    WHEN (c.member_connected_at IS NOT NULL AND c.coach_connected_at IS NOT NULL)
    THEN
    datediff(minute,
        GREATEST(c.coach_connected_at, c.member_connected_at),
        LEAST(c.coach_last_disconnect_at, c.member_last_disconnect_at)
    )
    ELSE NULL
  END AS platform_duration_minutes,
  TO_BOOLEAN(ca.responses:encountered_call_issue::VARCHAR) AS coach_encountered_call_issue, -- convert 0 to false, 1 to true, NULL stays NULL
  ca.responses:call_issue::VARCHAR AS coach_feedback_call_quality,
  TO_BOOLEAN(ma.responses:encountered_call_issue::INT) AS member_encountered_call_issue, -- convert 0 to false, 1 to true, NULL stays NULL
  ma.responses:call_issue:VARCHAR AS member_feedback_call_quality,
  ca.responses:covid_topics::VARCHAR AS coach_feedback_covid_topics,
  ca.responses:covid_session_extent::INT AS coach_feedback_covid_session_extent
FROM calls AS c
INNER JOIN sessions AS s ON
  c.call_id = s.call_id
LEFT OUTER JOIN coach_post_session_assessments AS ca
  ON s.session_id = ca.session_id
LEFT OUTER JOIN member_post_session_assessments AS ma
  ON s.session_id = ma.session_id
