WITH track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

tracks AS (

  SELECT * FROM {{ref('stg_app__tracks')}}

),

completed_sessions AS (

  SELECT * FROM {{ref('dbt_completed_sessions')}}

),

open_track_assignment_count_per_track AS (

  SELECT
    track_id,
    COUNT(CASE WHEN ended_at IS NULL THEN track_assignment_id ELSE NULL END) AS open_track_assignment_count
  FROM track_assignments
  GROUP BY track_id

),

start_end_date_per_track AS (

  SELECT
    track_id,
    MIN(created_at) AS start_date,
    MAX(ended_at) AS end_date
  FROM track_assignments
  GROUP BY track_id

),

members_with_completed_sessions_per_track AS (

  SELECT
    -- use COUNT(DISTINCT) since every member in this table has completed a session.
    track_id,
    COUNT(DISTINCT member_id) AS engaged_member_count -- use product-specific definition of engaged (>1 session)
  FROM completed_sessions
  GROUP BY track_id

)


SELECT
  t.track_id,
  o.open_track_assignment_count,
  s.start_date,
  s.end_date,
  COALESCE(m.engaged_member_count, 0) AS engaged_member_count,
  'https://app.betterup.co/admin/tracks/' || t.track_id::VARCHAR AS admin_panel_url,
  'https://app.betterup.co/frontend/partner/programs/members?track_id=' || t.track_id::VARCHAR AS partner_panel_url
FROM tracks AS t
INNER JOIN open_track_assignment_count_per_track AS o
  ON t.track_id = o.track_id
LEFT JOIN start_end_date_per_track AS s
  ON t.track_id = s.track_id
LEFT JOIN members_with_completed_sessions_per_track AS m
  ON t.track_id = m.track_id
