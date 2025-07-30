WITH growth_map_created AS (

  SELECT * FROM {{ source('segment_backend', 'growth_map_created') }}

)

SELECT
  id,
  user_id,
  track_assignment_id,
  product_subscription_assignment_id,
  event,
  context_library_name,
  deployment_type,
  to_timestamp(timestamp) AS event_at
FROM growth_map_created