{{
  config(
    tags=["eu"]
  )
}}

WITH src_coach_profile_pay_rates AS (
    SELECT * FROM {{ source('coach', 'coach_profile_pay_rates') }}
),
profile_pay_rates AS (
    SELECT
         uuid as coach_profile_pay_rate_uuid
        , id as coach_profile_pay_rate_id
        , adjustment_usd
        , base_pay_rate_id
        , coach_profile_uuid
        , {{ load_timestamp('created_at') }}
        , {{ load_timestamp('updated_at') }}
        , adjustment_usd_backup
    FROM src_coach_profile_pay_rates
)

SELECT * FROM profile_pay_rates
