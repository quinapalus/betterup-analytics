WITH billable_events AS (

  SELECT * FROM {{ref('dbt_billable_events')}}

)


SELECT
  -- first cast json to text so that it can be cast to int
  response_body:payment_id::INT AS shl_payment_id,
  billable_event_id,
  coach_id,
  member_id,
  track_id,
  event_type,
  augmented_event_type,
  {{ is_coach_cost_manual_upload ('event_type') }} AS is_coach_cost_manual_upload,
  event_at,
  CASE
    WHEN {{ is_coach_cost_manual_upload ('event_type') }} THEN DATEADD('MONTH', -1, sent_to_processor_at)
    ELSE sent_to_processor_at
  END AS event_reported_at,
  session_requested_length,
  usage_minutes,
  sent_to_processor_at,
  -- cast to numeric for easier processing in downstream dei model
  response_body:amount::FLOAT AS shl_payment_local_amount,
  response_body:currency::VARCHAR AS shl_payment_local_currency
FROM billable_events
WHERE response_body:payment_id::INT IS NOT NULL
-- filter out pre-Shortlist payment periods
AND sent_to_processor_at >= '02/01/2018'
