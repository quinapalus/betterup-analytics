{{
  config(
    tags=["eu"]
  )
}}

WITH partner_configurations AS (

  SELECT * FROM {{ source('app', 'partner_configurations') }}

)

SELECT
    id AS partner_configuration_id,
    downloadable,
    competency_mapping_id,
    partner_emails
FROM partner_configurations