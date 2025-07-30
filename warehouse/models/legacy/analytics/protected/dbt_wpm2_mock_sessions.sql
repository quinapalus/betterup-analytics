WITH sessions AS (

  SELECT * FROM {{ref('stg_app__sessions')}}

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}

),

user_info AS (

  SELECT * FROM {{ref('stg_app__users')}}

)

SELECT * FROM (

SELECT
  -- Select earliest mock session that's not canceled or missed.
  -- Note we do member_id -> coach_id aliasing here because the
  -- Coach/Coach Applicant schedules as the "member" for the Mocks.
  -- There's enough non-standard in the Mock Session setup that we
  -- go back to base `app` tables for processing here.
  s.member_id AS coach_id,
  c.email AS coach_email,
  s.starts_at,
  ROW_NUMBER() OVER (
      PARTITION BY s.member_id
      ORDER BY s.member_id, s.starts_at
  ) as index
FROM sessions AS s
INNER JOIN user_info AS c
  ON s.member_id = c.user_id
INNER JOIN track_assignments AS ta
  ON s.track_assignment_id = ta.track_assignment_id
INNER JOIN tracks AS t
  ON ta.track_id = t.track_id
WHERE t.name LIKE 'Whole Person Model Mock Sessions%'
  AND NOT s.missed
  AND s.canceled_at IS NULL

) a

WHERE index = 1
