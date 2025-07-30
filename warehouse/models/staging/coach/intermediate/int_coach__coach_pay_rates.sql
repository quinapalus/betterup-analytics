{{
  config(
    tags=["eu"]
  )
}}

WITH users AS (
    SELECT * FROM {{ref('stg_app__users')}}
),

coach_profiles AS (
    SELECT * FROM {{ref('stg_coach__coach_profiles')}}
),

coach_profile_pay_rates AS (
    SELECT * FROM {{ref('stg_coach__coach_profile_pay_rates')}}
),

base_pay_rates AS (
    SELECT * FROM {{ref('stg_coach__base_pay_rates')}}
),

coach_growth_path_pay_rates AS (
    SELECT * FROM {{ref('stg_app__coach_growth_path_pay_rates')}}
)

SELECT u.user_uuid,
       cp.coach_profile_uuid,
       cp.staffing_tier,
       bpr.name AS base_payment_name,
       bpr.type AS base_payment_type,
       bpr.coaching_cloud,
       bpr.amount_usd,
       IFF((base_payment_type = 'BasePayRates::PrimaryBasePayRate' AND bpr.coaching_cloud = 'professional' AND
            cp.staffing_tier IS NOT NULL), cgppr.amount_usd, 0) AS growth_pay_rate,
       cppr.adjustment_usd
FROM users u
         INNER JOIN coach_profiles cp ON cp.coach_profile_uuid = u.coach_profile_uuid
         INNER JOIN coach_profile_pay_rates cppr ON cppr.coach_profile_uuid = cp.coach_profile_uuid
         INNER JOIN base_pay_rates bpr ON bpr.base_pay_rate_id = cppr.base_pay_rate_id
         LEFT JOIN coach_growth_path_pay_rates cgppr ON (cgppr.tier = cp.staffing_tier AND cgppr.coaching_cloud = bpr.coaching_cloud)