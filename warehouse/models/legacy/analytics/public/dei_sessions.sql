WITH sessions AS (

  SELECT * FROM {{ref('stg_app__sessions')}}

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

),

specialist_verticals AS (

  SELECT * FROM {{ref('stg_curriculum__specialist_verticals')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  where is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

)


SELECT
  s.session_id,
  s.member_id,
  s.coach_id,
  ta.track_id,
  s.created_at AS scheduled_at,
  s.requested_length,
  s.recurring,
  s.starts_at,
  s.ends_at,
  s.canceled_at,
  s.canceled_at IS NOT NULL AS canceled,
  s.complete_at IS NOT NULL AS completed,
  s.missed,
  s.coach_assignment_id,
  s.track_assignment_id,
  ca.role AS coach_type,
  sv.name AS extended_network_session_type,
  s.call_id,
  s.development_topic_id
FROM sessions AS s
INNER JOIN track_assignments AS ta
  ON s.track_assignment_id = ta.track_assignment_id
INNER JOIN coach_assignments AS ca
  ON s.coach_assignment_id = ca.coach_assignment_id
LEFT OUTER JOIN specialist_verticals AS sv
  ON ca.specialist_vertical_uuid = sv.specialist_vertical_uuid
WHERE ta.track_id IN (SELECT track_id FROM tracks)
