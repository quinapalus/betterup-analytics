{{
  config(
    tags=["eu"]
  )
}}

WITH

product_subscription_assignments AS (
  SELECT * FROM {{ref('stg_app__product_subscription_assignments')}}
),

destroyed_records AS (
  SELECT * 
  FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'ProductSubscriptionAssignment'
)

SELECT
  psa.product_subscription_assignment_id,
  psa.product_subscription_id,
  psa.member_id,
  psa.stripe_subscription_id,
  psa.stripe_data,
  psa.stripe_customer_id,
  psa.v2,
  psa.created_at,
  psa.updated_at,
  psa.starts_at,
  psa.ended_at,
  psa.ends_at,
  psa.requested_cancellation_at
FROM product_subscription_assignments AS psa
LEFT JOIN destroyed_records AS v
  ON psa.product_subscription_assignment_id = v.item_id
WHERE v.item_id IS NULl AND
  ( DATEDIFF(minute, starts_at, COALESCE(ended_at, ends_at)) > 60 OR
    COALESCE(ended_at, ends_at) IS NULL
  )
