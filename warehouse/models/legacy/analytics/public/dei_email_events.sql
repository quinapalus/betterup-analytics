-- We break convention here to specify specifically which
-- columns are needed. We are merging a historical dataset
-- from stitch with once sync'd from Segment.
-- Unioning the tables requires the exact same columns to
-- be specified.
WITH sgd_email_events AS (

  SELECT
    email,
    event_id,
    message_id,
    event,
    event_at,
    tags,
    useragent,
    url,
    reason,
    null as comms_tracking_number -- stg_sendgrid__email_events contains historical data which predates the comms_tracking_number column
  FROM {{ref('int_events__emails')}}

),

seg_email_events AS (

  SELECT
    email,
    event_id,
    message_id,
    event,
    event_at,
    tags,
    useragent,
    url,
    reason,
    comms_tracking_number
  FROM {{ref('stg_segment_sendgrid__email_events')}}

),

combined_email_events AS (

  SELECT * FROM sgd_email_events
  UNION
  SELECT * FROM seg_email_events

),

user_info AS (

  SELECT * FROM {{ref('stg_app__users')}}

)


SELECT
  {{dbt_utils.surrogate_key(['e.event_id', 'e.tags', 'e.event_at']) }} as _unique,
  a.user_id,
  e.event_id as sendgrid_event_id,
  e.message_id as sendgrid_message_id,
  e.event,
  e.event_at,
  e.tags,
  e.useragent,
  e.url,
  e.reason,
  e.comms_tracking_number
FROM
  combined_email_events AS e
LEFT OUTER JOIN
  user_info AS a
ON
  a.email = e.email
{% if is_incremental() %}
WHERE
  event_at > (SELECT max(event_at) FROM {{this}})
{% endif %}
