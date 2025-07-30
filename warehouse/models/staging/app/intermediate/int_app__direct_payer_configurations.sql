{{
  config(
    tags=["eu"]
  )
}}

WITH direct_payer_configurations AS (

  SELECT * FROM {{ ref('stg_app__direct_payer_configurations') }}

),

experience_configuration_assignments AS (

  SELECT * FROM {{ ref('stg_app__experience_configuration_assignments') }}
          WHERE configurable_type = 'ExperienceConfigurations::DirectPayerConfiguration'

)


SELECT e.track_id, c.*, {{ dbt_utils.surrogate_key(['e.track_id', 'c.direct_payer_configuration_id']) }} AS primary_key
   FROM experience_configuration_assignments AS e
LEFT OUTER JOIN direct_payer_configurations AS c
   ON c.direct_payer_configuration_id = e.configurable_id
QUALIFY ROW_NUMBER() OVER
   (PARTITION BY e.track_id ORDER BY e.created_at DESC) = 1 -- logic that limits to unique record per track; picks up latest config
