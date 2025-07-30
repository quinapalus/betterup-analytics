{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH whole_person_subdimensions AS (

  SELECT * FROM {{ref('whole_person_subdimensions')}}

)

SELECT
  -- create unique surrogate key using WPM version and subdimension key
  -- to account for overlapping subdimension_keys between 1.0 and 2.0
  {{ dbt_utils.surrogate_key(['whole_person_model_version', 'assessment_subdimension_key']) }}
    AS whole_person_subdimension_key,
  *
FROM whole_person_subdimensions
