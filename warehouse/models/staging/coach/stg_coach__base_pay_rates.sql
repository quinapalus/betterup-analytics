{{
  config(
    tags=["eu"]
  )
}}

WITH src_base_pay_rates AS (
    SELECT * FROM {{ source('coach', 'base_pay_rates') }}
),
base_pay_rates AS (
    SELECT
        id as base_pay_rate_id
        , uuid as base_pay_rate_uuid
        , specialist_vertical_uuid
        , specialist_vertical_id  --still functional but only for US data
        , {{ load_timestamp('created_at') }}
        , {{ load_timestamp('updated_at') }}
        , coaching_cloud
        , hiring_tier
        , name
        , type
        , amount_usd

    FROM src_base_pay_rates
)


SELECT * FROM base_pay_rates
