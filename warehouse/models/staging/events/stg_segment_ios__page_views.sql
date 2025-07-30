WITH page_views AS (

  SELECT * FROM {{ source('segment_ios', 'page_views') }}

)


SELECT
  id AS event_id,
  user_id,
  event_text,
  received_at,
  page_name,
  NULL AS page_url,
  'ios' AS platform,
  client_features_enabled,
  server_features_enabled,
  -- iOS events don't have user agent or transition time information
  NULL AS user_agent,
  NULL AS transition_time
FROM page_views
