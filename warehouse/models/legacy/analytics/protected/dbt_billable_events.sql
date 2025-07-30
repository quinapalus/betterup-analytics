WITH billable_events AS (

  SELECT * FROM {{ref('app_billable_events')}}

),

sessions AS (

  SELECT * FROM {{ref('stg_app__sessions')}}

),

coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

)


SELECT
  be.billable_event_id,
  be.associated_record_id,
  be.coach_id,
  be.member_id,
  be.track_id,
  be.event_type,
  {{ augment_billable_event_type ('be.event_type', 'ca.role') }} AS augmented_event_type,
  be.event_at,
  CASE
    WHEN be.event_type = 'completed_sessions' THEN s.requested_length
    ELSE NULL
  END AS session_requested_length,
  be.usage_minutes,
  be.sent_to_processor_at,
  be.response_body
FROM billable_events AS be
LEFT OUTER JOIN sessions AS s
  ON be.associated_record_type = 'Session'
  AND be.associated_record_id = s.session_id
LEFT OUTER JOIN coach_assignments AS ca
  ON s.coach_assignment_id = ca.coach_assignment_id
