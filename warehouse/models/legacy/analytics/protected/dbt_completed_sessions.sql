WITH billable_events AS (

  SELECT * FROM {{ref('stg_app__billable_events')}}

)

SELECT * FROM (

SELECT
  -- select the first instance of completed session for
  -- records with multiple associated completed sessions.
  associated_record_id AS session_id,
  billable_event_id,
  member_id,
  coach_id,
  track_id,
  event_at AS starts_at,
  usage_minutes AS reported_duration_minutes,
  ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY event_at) AS member_completed_session_sequence,
  ROW_NUMBER() OVER (
      PARTITION BY associated_record_id
      ORDER BY associated_record_id, created_at ASC
  ) AS index
FROM billable_events
WHERE event_type = 'completed_sessions'
  AND associated_record_type = 'Session'

) a

WHERE index = 1
