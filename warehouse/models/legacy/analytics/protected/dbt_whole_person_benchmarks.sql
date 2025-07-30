{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH whole_person_benchmarks AS (

  SELECT * FROM {{ref('bu_whole_person_benchmarks')}}

)


SELECT
  -- create unique surrogate key using WPM version and subdimension key
  -- in order to join benchmark data to dim_whole_person_subdimension
  -- currently benchmark data
  {{ dbt_utils.surrogate_key(['whole_person_model_version', 'construct_key']) }} AS whole_person_subdimension_key,
  *
FROM whole_person_benchmarks
