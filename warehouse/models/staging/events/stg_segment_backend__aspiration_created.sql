WITH aspiration_created AS (

  SELECT * FROM {{ source('segment_backend', 'aspiration_created') }}

)

SELECT
  id,
  user_id,
  aspiration_id,
  aspiration_growth_focus_area_selection_id,
  aspiration_aspiration_timeframe_translation_key,
  aspiration_aspiration_timeframe_duration,
  aspiration_description,
  track_assignment_id,
  product_subscription_assignment_id,
  event,
  context_library_name,
  deployment_type,
  to_timestamp(timestamp) AS event_at
FROM aspiration_created
WHERE aspiration_id IS NOT NULL