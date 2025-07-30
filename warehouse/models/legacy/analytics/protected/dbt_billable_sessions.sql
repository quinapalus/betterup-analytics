WITH billable_events AS (

  SELECT * FROM {{ref('app_billable_events')}}

)

SELECT
  billable_event_id,
  event_type,
  event_at,
  usage_minutes,
  member_id,
  coach_id,
  track_id,
  track_assignment_id,
  associated_record_id AS session_id, -- these ids will be NULL for late_cancellations before Feb 2018
  created_at,
  updated_at
FROM billable_events
WHERE usage_minutes > 0
