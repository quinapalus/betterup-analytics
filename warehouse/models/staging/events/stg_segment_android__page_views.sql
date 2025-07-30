WITH page_views AS (

  SELECT * FROM {{ source('segment_android', 'page_views') }}

)


SELECT
  id AS event_id,
  user_id,
  event_text,
  received_at,
  page_name,
  NULL AS page_url,
  'android' AS platform,
  client_features_enabled,
  server_features_enabled,
  context_user_agent AS user_agent,
  -- andorid events don't have transition time information
  NULL AS transition_time
FROM page_views
