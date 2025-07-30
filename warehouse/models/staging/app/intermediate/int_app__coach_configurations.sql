{{
  config(
    tags=["eu"]
  )
}}

WITH coach_configurations AS (

  SELECT * FROM {{ ref('stg_app__coach_configurations') }}

),

experience_configuration_assignments AS (

  SELECT * FROM {{ ref('stg_app__experience_configuration_assignments') }}
          WHERE configurable_type = 'ExperienceConfigurations::CoachConfiguration'

)


SELECT e.track_id, c.*, {{ dbt_utils.surrogate_key(['e.track_id', 'c.coach_configuration_id']) }} AS primary_key
   FROM experience_configuration_assignments AS e
LEFT OUTER JOIN coach_configurations AS c
   ON c.coach_configuration_id = e.configurable_id
QUALIFY ROW_NUMBER() OVER
   (PARTITION BY e.track_id ORDER BY e.created_at DESC) = 1 -- logic that limits to unique record per track; picks up latest config
