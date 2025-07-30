{{
  config(
    materialized='table'
  )
}}


WITH events AS (

  SELECT * FROM {{ source('mixpanel', 'events') }}

)


SELECT
  "_rjm_record_hash" AS event_id,
  try_cast("distinct_id" as INTEGER) AS user_id,
  {{convert_timezone('"time"', "UTC")}} as event_at,
  replace(lower(trim("event")), ' ', '_') as event,
  "event" as event_text --Renamed to match Segment column name
FROM events
WHERE user_id is not null
-- Throw away any rows with non-integer ids (happens before `track` call is made). 
-- With try_cast, anything non-numeric returns as null.
