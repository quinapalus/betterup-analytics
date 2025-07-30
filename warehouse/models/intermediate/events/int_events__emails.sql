WITH events AS (

  SELECT * FROM {{ ref('stg_sendgrid__events') }}

),

event_categories AS (

  SELECT * FROM {{ ref('stg_sendgrid__event_categories') }}

)


SELECT
  {{ dbt_utils.surrogate_key(['e.sg_event_id', 'e.event_at', 'c.value']) }} as int_events__emails_id,
  e.sg_event_id AS event_id,
  e.sg_message_id AS message_id,
  e.email,
  e.event,
  to_timestamp(e.event_at) AS event_at,
  c.value AS tags,
  e.useragent,
  e.url,
  e.reason
  
FROM events AS e
LEFT JOIN event_categories AS c
ON
  e.email = c._sdc_source_key_email AND
  e.event = c._sdc_source_key_event AND
  e.event_at = c._sdc_source_key_timestamp
ORDER BY e.event_at DESC
