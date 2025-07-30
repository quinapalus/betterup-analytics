WITH billable_sessions AS (

  SELECT * FROM {{ref('dbt_billable_sessions')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}

)


SELECT
  {{ date_key('event_at') }} AS date_key,
  coach_id AS app_coach_id,
  SUM(usage_minutes / 60.0) AS billable_session_hours,
  COUNT(DISTINCT billable_event_id) AS billable_session_count,
  SUM(CASE WHEN event_type = 'completed_sessions' THEN usage_minutes / 60.0 ELSE 0 END)
    AS completed_session_hours,
  COUNT(CASE WHEN event_type = 'completed_sessions' THEN session_id END)
    AS completed_session_count
FROM billable_sessions
WHERE
  -- filter for billable_sessions on non-QA tracks
  track_id IN (SELECT track_id FROM tracks WHERE deployment_type <> 'qa')
GROUP BY {{ date_key('event_at') }}, coach_id
