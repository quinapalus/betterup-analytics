WITH events AS (

  SELECT * FROM {{ source('segment_backend', 'events') }}

)


SELECT
  id AS event_id,
  user_id::INTEGER AS user_id,
  {{convert_timezone("timestamp", "UTC")}} as event_at,
  event,
  event_text,
  context_library_name,
  context_library_version
  -- original_timestamp::timestamp
  -- uuid_ts Not exactly sure what this is used for?
  -- received_at::timestamp, Not useful for analytics
  -- sent_at::timestamp, Not useful for analytics
  -- completed_at::timestamp,
FROM events
