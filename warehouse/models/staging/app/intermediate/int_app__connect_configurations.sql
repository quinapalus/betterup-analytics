{{
  config(
    tags=["eu"]
  )
}}

WITH
experience_configuration_assignments AS (
  SELECT * FROM {{ ref('stg_app__experience_configuration_assignments') }}
          WHERE configurable_type = 'ExperienceConfigurations::connectConfiguration'
)


SELECT
    e.track_id
FROM experience_configuration_assignments AS e
QUALIFY ROW_NUMBER() OVER
   (PARTITION BY e.track_id ORDER BY e.created_at DESC) = 1 -- logic that limits to unique record per track; picks up latest config
