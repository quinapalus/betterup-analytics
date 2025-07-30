WITH track_enrollments AS (

  SELECT * FROM {{ref('dbt_track_enrollments')}}
  -- only look at currently open members
  WHERE ended_at IS NULL

),

members AS (

  SELECT * FROM {{ref('dei_members')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

sessions AS (

  SELECT * FROM {{ref('dei_sessions')}}

)


SELECT
  te.member_id,
  te.track_id,
  t.contract_id,
  t.organization_id,
  t.sfdc_account_id,
  -- infer from dei_members whether the member has
  -- an upcoming session on the given track.
  m.next_session_id IS NOT NULL AS has_upcoming_session,
  s.recurring is_upcoming_session_recurring,
  s.coach_type AS upcoming_session_type,
  s.starts_at AS upcoming_session_starts_at
FROM track_enrollments AS te
INNER JOIN tracks AS t
  ON te.track_id = t.track_id
INNER JOIN members AS m
  ON te.member_id = m.member_id
LEFT OUTER JOIN sessions AS s
  ON m.next_session_id = s.session_id
