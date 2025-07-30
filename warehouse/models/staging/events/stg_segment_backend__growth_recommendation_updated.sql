WITH growth_recommendation_updated AS (

  SELECT * FROM {{ source('segment_backend', 'growth_recommendation_updated') }}

)

SELECT
  id,
  user_id,
  growth_recommendation_recommended_item_id,
  track_assignment_id,
  product_subscription_assignment_id,
  event,
  context_library_name,
  to_timestamp(timestamp) AS event_at,
  changes_dismissed_at,
  changes_completed_at
FROM growth_recommendation_updated