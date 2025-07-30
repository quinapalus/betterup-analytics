{{
  config(
    tags=["eu"]
  )
}}

WITH src_coach_growth_path_pay_rates AS (
    SELECT * FROM {{ source('app', 'coach_growth_path_pay_rates') }}
),
growth_path_pay_rates AS (
    SELECT
      id as coach_growth_path_pay_rate_id
      , amount_usd
      , tier
      , name
      , {{ load_timestamp('created_at') }}
      , {{ load_timestamp('updated_at') }}
      , coaching_cloud
      , uuid as coach_growth_path_pay_rate_uuid
    FROM src_coach_growth_path_pay_rates
)


SELECT * FROM growth_path_pay_rates
