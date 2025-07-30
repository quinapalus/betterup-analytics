WITH events AS (

  SELECT * FROM {{ source('segment_android', 'events') }}

)


SELECT
  id AS event_id,
  user_id::STRING AS user_id,
  {{convert_timezone("timestamp", "UTC")}} as event_at,
  event,
  event_text,
  'Android' AS platform,
  context_app_build,
  context_app_name,
  context_app_namespace,
  context_app_version,
  context_device_ad_tracking_enabled,
  context_device_id,
  context_device_manufacturer,
  context_device_model,
  context_device_type,
  context_library_name,
  context_library_version,
  context_locale,
  context_network_cellular,
  context_network_wifi,
  context_network_bluetooth,
  context_network_carrier,
  context_os_name,
  context_os_version,
  context_screen_density,
  context_screen_height,
  context_screen_width,
  context_timezone as context_tz_iana,
  context_user_agent,
  anonymous_id AS anonymous_user_token
  --context_ip, Don't need to show the IP information
  -- uuid_ts
  -- received_at::timestamp, Not useful for analytics
  -- sent_at::timestamp, Not useful for analytics
  -- completed_at::timestamp,
  -- original_timestamp::timestamp
FROM events
