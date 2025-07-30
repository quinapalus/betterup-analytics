WITH member_dates_on_track AS (

  SELECT * FROM {{ref('dbt_member_dates_on_track')}}

),

member_dates_on_track_sessions AS (

  SELECT * FROM {{ref('dbt_member_dates_on_track_sessions')}}

),

members AS (

  SELECT * FROM {{ref('dei_members')}}

),

member_track_invite AS (

  SELECT
    member_id,
    track_id,
    MIN(date_day) AS invite_date
  FROM member_dates_on_track
  GROUP BY member_id, track_id

),

member_track_session AS (

  SELECT
    member_id,
    track_id,
    MAX(completed_session_count_track_to_date) AS completed_session_count
  FROM member_dates_on_track_sessions
  GROUP BY member_id, track_id

)


SELECT
  {{ dbt_utils.surrogate_key(['mti.member_id', 'mti.track_id']) }} as primary_key,
  mti.member_id,
  mti.track_id,
  mti.invite_date,
  m.activated_at IS NOT NULL AS is_activated,
  DATE_TRUNC('DAY', m.activated_at) AS activation_date,
  mts.completed_session_count
FROM member_track_invite AS mti
INNER JOIN member_track_session AS mts
  ON mti.member_id = mts.member_id
  AND mti.track_id = mts.track_id
LEFT OUTER JOIN members AS m
  ON mti.member_id = m.member_id
