WITH assessment_item_response_submitted AS (

  SELECT * FROM {{ source('segment_backend', 'assessment_item_response_submitted') }}

)

SELECT
  id,
  user_id,
  assessment_id,
  assessment_item_id,
  assessment_platform,
  key,
  value,
  item_type,
  track_assignment_id,
  product_subscription_assignment_id,
  event,
  context_library_name,
  deployment_type,
  to_timestamp(timestamp) AS event_at
FROM assessment_item_response_submitted