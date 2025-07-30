{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH consumer_subscription AS (

  SELECT * FROM {{ref('stg_app__consumer_subscriptions')}}
  WHERE date(created_at) <= '2022-01-12'

),

psa AS (

  SELECT * FROM {{ref('int_app__product_subscription_assignments')}}
  WHERE stripe_subscription_id IS NOT NULL
  AND stripe_subscription_id NOT IN (
    SELECT stripe_subscription_id FROM consumer_subscription
  )

),

event_subscription_created AS (

  SELECT * FROM {{ref('stg_segment_backend__subscription_created')}}

),

event_subscription_plan_switch AS (

  SELECT * FROM {{ref('stg_segment_backend__subscription_plan_switch')}}

),

psa_most_recent_susbcription AS (

  SELECT * FROM (
    SELECT *, row_number() over(partition by stripe_subscription_id order by created_at desc) as rn
    FROM psa
  )
  WHERE rn = 1

)

SELECT
cs.app_subscription_id as subscription_id,
null as product_subscription_assignment_id,
cs.stripe_subscription_id,
cs.stripe_data::varchar as stripe_data,
try_to_number(e.user_id) as member_id,
cs.track_assignment_id,
cs.created_at,
cs.updated_at,
cs.ended_at,
e.event_text,
e.deployment_type,
e.track_id,
e.consumer_product_price_unit_amount,
e.consumer_product_price_nickname,
{# e.consumer_product_id,
e.consumer_product_price_recurring_interval,
e.consumer_product_price_recurring_interval_count,
e.consumer_product_name,
e.consumer_product_sessions_per_month,
e.upfront_subscription_state, #}
e.hubspot_contact_id,
e.browser_name,
e.browser_version,
e.platform_version,
e.device_name,
e.platform_name,
e.app_version,
e.app_build
FROM consumer_subscription cs
LEFT JOIN event_subscription_created e ON cs.track_assignment_id = e.track_assignment_id and to_char(cs.created_at, 'YYYY-MM-DD HH24:MI') = to_char(e.original_timestamp, 'YYYY-MM-DD HH24:MI')

UNION

SELECT
null as subscription_id,
s.product_subscription_assignment_id,
s.stripe_subscription_id,
s.stripe_data::varchar as stripe_data,
try_to_number(s.member_id),
e.track_assignment_id,
s.created_at,
s.updated_at,
s.ended_at,
e.event_text,
e.deployment_type,
e.track_id,
e.target_price_unit_amount,
e.target_price_nickname,
{# e.consumer_product_id,
e.consumer_product_price_recurring_interval,
e.consumer_product_price_recurring_interval_count,
e.consumer_product_name,
e.consumer_product_sessions_per_month,
e.upfront_subscription_state, #}
e.hubspot_contact_id,
e.browser_name,
e.browser_version,
e.platform_version,
e.device_name,
e.platform_name,
e.app_version,
e.app_build
FROM psa_most_recent_susbcription s
LEFT JOIN event_subscription_plan_switch e ON s.product_subscription_assignment_id = e.product_subscription_assignment_id
WHERE e.product_subscription_assignment_id IS NOT NULL
