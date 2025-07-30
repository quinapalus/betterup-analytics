{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH events AS(
  SELECT *
  FROM {{ source('stripe', 'events') }}
)
, event_data AS(
  SELECT data, id, created, type
  FROM events
)
, lateral_flattened_data AS(
    SELECT t.key AS t_key, t.value AS t_value, id, created, type
    FROM (
        SELECT * FROM event_data)
    ,LATERAL FLATTEN(input => data) t
)

SELECT DISTINCT id,
created,
type,
(parse_json(t_value):"subscription"::varchar) AS stripe_subscription_id,
(parse_json(t_value):"customer_email"::varchar) AS customer_email,
(parse_json(t_value):"customer"::varchar) AS customer_id
FROM(
  SELECT * FROM lateral_flattened_data
  WHERE t_key = 'object'
)
