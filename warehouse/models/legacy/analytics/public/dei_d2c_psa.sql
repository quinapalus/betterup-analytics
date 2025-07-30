{{
  config(
    tags=['classification.c3_confidential'],
    materialized='view'
  )
}}

WITH product_subscription_assignments AS (

  SELECT * FROM {{ ref('stg_app__product_subscription_assignments') }}
  WHERE stripe_subscription_id IS NOT NULL

)

SELECT
  psa.product_subscription_assignment_id,
  psa.product_subscription_id,
  psa.member_id,
  psa.stripe_subscription_id,
  psa.stripe_data,
  psa.created_at,
  psa.updated_at,
  psa.starts_at,
  psa.ended_at,
  psa.ends_at,
  psa.requested_cancellation_at
FROM product_subscription_assignments AS psa
