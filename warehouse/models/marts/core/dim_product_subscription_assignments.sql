{{
  config(
    tags=["eu"]
  )
}}

WITH
product_subscription_assignments AS (
  SELECT * FROM {{ ref('int_app__product_subscription_assignments') }}
  ),

product_subscriptions AS (
  SELECT * FROM {{ ref('stg_app__product_subscriptions') }}
  ),

products AS (
  SELECT * FROM {{ ref('stg_app__products') }}
),

final AS (
  SELECT
    psa.product_subscription_assignment_id,
    psa.product_subscription_id,
    psa.member_id,
    psa.created_at,
    psa.updated_at,
    psa.starts_at,
    psa.ended_at,
    psa.ends_at,
    psa.v2,
    CASE WHEN psa.starts_at <= CURRENT_DATE THEN TRUE ELSE FALSE END AS has_started,
    DATEDIFF(day, psa.starts_at, psa.ends_at) AS planned_subscription_length_days,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 30 THEN TRUE ELSE FALSE END AS is_planned_subscription_2_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 60 THEN TRUE ELSE FALSE END AS is_planned_subscription_3_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 90 THEN TRUE ELSE FALSE END AS is_planned_subscription_4_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 120 THEN TRUE ELSE FALSE END AS is_planned_subscription_5_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 150 THEN TRUE ELSE FALSE END AS is_planned_subscription_6_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 180 THEN TRUE ELSE FALSE END AS is_planned_subscription_7_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 210 THEN TRUE ELSE FALSE END AS is_planned_subscription_8_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 240 THEN TRUE ELSE FALSE END AS is_planned_subscription_9_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 270 THEN TRUE ELSE FALSE END AS is_planned_subscription_10_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 300 THEN TRUE ELSE FALSE END AS is_planned_subscription_11_month,
    CASE WHEN DATEDIFF(day, psa.starts_at, psa.ends_at) > 330 THEN TRUE ELSE FALSE END AS is_planned_subscription_12_month,
    CASE WHEN psa.ended_at IS NULL AND psa.starts_at <= CURRENT_DATE THEN TRUE ELSE FALSE END AS is_active,
    ps.product_id,
    ps.organization_id,
    p.source
  FROM product_subscription_assignments AS psa
  INNER JOIN product_subscriptions AS ps
    ON psa.product_subscription_id = ps.product_subscription_id
  INNER JOIN products AS p
    ON ps.product_id = p.product_id
  )

select * from final
