WITH engagement_events AS (

  SELECT * FROM {{ source('app', 'engagement_events') }}

)

SELECT
  id AS engagement_event_id,
  track_assignment_id, 
  TO_DATE(event_at) AS date,
  to_char(event_at, 'YYYYMMDD') AS date_key,
  user_id,
  eventable_id,
  eventable_type,
  verb,
  eventable_subject,
  {{ load_timestamp('event_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM engagement_events
WHERE NOT (eventable_type = 'RatedResource' AND event_at > '2020-02-03' AND event_at < '2020-02-05')
