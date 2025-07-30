{{
  config(
    tags=["eu"]
  )
}}

WITH direct_payer_configurations AS (

  SELECT * FROM {{ source('app', 'direct_payer_configurations') }}

)


SELECT
  id AS direct_payer_configuration_id,
  stripe_pricing_plan_id
FROM direct_payer_configurations