WITH src_eu_staging_coach_pay_rates AS (
    SELECT* FROM {{ source('analytics_eu_staging', 'int_coach__coach_pay_rates') }}
),

coach_pay_rates AS (
    SELECT
        user_uuid
        , coach_profile_uuid
        , staffing_tier
        , base_payment_name
        , base_payment_type
        , coaching_cloud
        , amount_usd
        , growth_pay_rate
        , adjustment_usd
    FROM src_eu_staging_coach_pay_rates

)

SELECT * FROM coach_pay_rates