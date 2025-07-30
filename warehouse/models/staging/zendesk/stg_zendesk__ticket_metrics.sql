{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH ticket_metrics AS (

  SELECT * FROM {{ source('zendesk', 'ticket_metrics') }}

)


SELECT
  --ids
  id::BIGINT AS ticket_metric_id,
  ticket_id::BIGINT AS ticket_id,

  --fields
  initially_assigned_at,
  assigned_at,
  solved_at,
  updated_at,
  status_updated_at,
  assignee_updated_at,
  requester_updated_at,
  reply_time_in_minutes_business AS time_to_reply_business_minutes,
  reply_time_in_minutes_calendar AS time_to_reply_calendar_minutes,
  first_resolution_time_in_minutes_business AS time_to_first_resolution_business_minutes,
  first_resolution_time_in_minutes_calendar AS time_to_first_resolution_calendar_minutes,
  full_resolution_time_in_minutes_business AS time_to_full_resolution_business_minutes,
  full_resolution_time_in_minutes_calendar AS time_to_full_resolution_calendar_minutes,
  replies AS replies_count,
  reopens AS reopens_count
FROM
  ticket_metrics
