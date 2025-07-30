WITH member_dates_on_track AS (

  SELECT * FROM {{ref('dbt_member_dates_on_track')}}

),

completed_sessions AS (

  SELECT * FROM {{ref('dbt_completed_sessions')}}

),

member_date_sessions AS (

  SELECT
    member_id,
    track_id,
    DATE_TRUNC('DAY', starts_at) AS date_day,
    COUNT(DISTINCT billable_event_id) AS completed_session_count
  FROM completed_sessions
  GROUP BY member_id, track_id, DATE_TRUNC('DAY', starts_at)

)


SELECT
  mt.member_id,
  mt.track_id,
  mt.date_day,
  -- use COALESCE to assign 0 sessions on days that
  -- do not exist in member_date_sessions for correct summing.
  SUM(COALESCE(completed_session_count, 0)) OVER (PARTITION BY mt.member_id, mt.track_id
     ORDER BY mt.date_day
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        AS completed_session_count_track_to_date
FROM member_dates_on_track AS mt
LEFT OUTER JOIN member_date_sessions AS ms
  ON mt.member_id = ms.member_id
  AND mt.track_id = ms.track_id
  AND mt.date_day = ms.date_day
-- define window function to sum the number
-- of completed sessions over time for
-- each member-track pair.

