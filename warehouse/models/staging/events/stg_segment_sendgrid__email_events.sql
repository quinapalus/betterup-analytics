{{
  config(
    materialized='ephemeral'
  )
}}

-- TODO (lmalott) remember to chagne materialization to ephermal

WITH events AS (

  SELECT * FROM {{ source('segment_sendgrid', 'activity') }}

)


SELECT
  sg_event_id AS event_id,
  sg_message_id AS message_id,
  email,
  event,
  to_timestamp(timestamp) AS event_at,
  category AS tags,
  useragent,
  url,
  url_offset_index,
  url_offset_type,
  reason,
  resource_ids,
  comms_tracking_number
FROM events
ORDER BY timestamp DESC
