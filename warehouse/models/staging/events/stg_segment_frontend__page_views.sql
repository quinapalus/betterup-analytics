WITH page_views AS (

  SELECT * FROM {{ source('segment_frontend', 'page_views') }}

)


SELECT
  id AS event_id,
  user_id,
  event_text,
  received_at,
  context_page_title AS page_name,
  page_url,
  'web' AS platform,
  client_features_enabled,
  server_features_enabled,
  context_user_agent AS user_agent,
  transition_time
FROM page_views
