WITH events_resource_served AS (

  SELECT * FROM {{ source('segment_backend', 'events_resource_served') }}

)


SELECT
  id AS event_id,
  user_id::INTEGER AS user_id,
  resource_id::INTEGER AS resource_id,
  {{convert_timezone("timestamp", "UTC")}} as event_at,
  event,
  event_text,
  context_library_name,
  context_library_version
FROM events_resource_served
