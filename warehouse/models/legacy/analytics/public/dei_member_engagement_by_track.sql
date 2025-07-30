{{
  config(
    tags=['eu']
  )
}}

WITH track_enrollments AS (

  SELECT * FROM {{ref('dbt_track_enrollments')}}

),

billable_sessions AS (

  SELECT * FROM {{ref('dbt_billable_sessions')}}

),

session_schedules AS (

  SELECT * FROM {{ref('dei_sessions')}} -- deviating from CTE-naming convention here for clarity against billable sessions.

),

members_with_upcoming_sessions AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    starts_at,
    session_id,
    row_number() over (
        PARTITION BY member_id
        ORDER BY member_id, starts_at
    ) AS index
  FROM session_schedules
  WHERE NOT canceled
    AND starts_at > current_timestamp

  ) a

  WHERE index = 1

),

session_engagement_by_track AS (

  SELECT
    member_id,
    track_id,
    SUM(usage_minutes) / 60.0 AS billable_hours_sum,
    COUNT(*) AS billable_session_count,
    MIN(event_at) AS first_billable_session_at,
    MAX(event_at) AS last_billable_session_at,
    SUM(CASE WHEN event_type = 'completed_sessions' THEN usage_minutes END) / 60.0 AS completed_session_hours_sum,
    COUNT(CASE WHEN event_type = 'completed_sessions' THEN session_id END) AS completed_session_count,
    AVG(CASE WHEN event_type = 'completed_sessions' THEN usage_minutes END) AS completed_session_minutes_mean,
    MIN(CASE WHEN event_type = 'completed_sessions' THEN event_at END) AS first_completed_session_at,
    MAX(CASE WHEN event_type = 'completed_sessions' THEN event_at END) AS last_completed_session_at
  FROM billable_sessions
  GROUP BY member_id, track_id

)


SELECT
  {{ dbt_utils.surrogate_key(['e.member_id', 'e.track_id']) }} as primary_key,
  e.member_id,
  e.track_id,
  e.invited_at,
  e.ended_at,
  e.coaching_hours_remaining,
  e.is_primary_coaching_enabled,
  e.is_on_demand_coaching_enabled,
  e.is_extended_network_coaching_enabled,
  COALESCE(s.billable_hours_sum, 0) AS billable_hours_sum,
  COALESCE(s.billable_session_count, 0) AS billable_session_count,
  s.first_billable_session_at,
  s.last_billable_session_at,
  COALESCE(s.completed_session_hours_sum, 0) AS completed_session_hours_sum,
  COALESCE(s.completed_session_count, 0) AS completed_session_count,
  s.completed_session_minutes_mean,
  s.first_completed_session_at,
  datediff('day', e.invited_at, s.first_completed_session_at) AS first_completed_session_days_after_invite,
  s.last_completed_session_at,
  -- if member closed on track, then no next session. Otherwise, use information from joined table.
  CASE
    WHEN e.ended_at IS NOT NULL THEN false
    ELSE (CASE WHEN m.session_id IS NULL THEN false ELSE true END)
  END AS has_upcoming,
  e.track_assignments_count
FROM track_enrollments AS e
LEFT OUTER JOIN members_with_upcoming_sessions AS m ON e.member_id = m.member_id
LEFT OUTER JOIN session_engagement_by_track AS s ON e.member_id = s.member_id AND e.track_id = s.track_id
