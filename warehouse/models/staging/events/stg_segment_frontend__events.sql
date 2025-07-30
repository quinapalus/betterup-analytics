WITH events AS (

  SELECT * FROM {{ source('segment_frontend', 'events') }}

)


SELECT
  id AS event_id,
  user_id::STRING AS user_id,
  {{convert_timezone("timestamp", "UTC")}} as event_at,
  event,
  event_text,
  'web' AS platform,
  context_campaign_medium,
  context_campaign_name,
  context_campaign_source,
  context_campaign_term,
  -- context_ip, Don't track IPs with events
  context_library_name,
  context_library_version
  context_page_path,
  context_page_referrer
  context_page_search,
  context_page_title,
  context_page_url,
  context_user_agent,
  anonymous_id AS anonymous_user_token
  -- received_at::timestamp, Not useful for analytics
  -- sent_at::timestamp, Not useful for analytics
  -- completed_at::timestamp,
  -- original_timestamp::timestamp
  -- uuid_ts
FROM events
