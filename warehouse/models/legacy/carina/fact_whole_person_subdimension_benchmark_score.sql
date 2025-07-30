{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH whole_person_benchmarks AS (

  -- filter for benchmarks with non-NULL industry and role values only
  SELECT * FROM {{ref('dbt_whole_person_benchmarks')}}
  WHERE industry IS NOT NULL
    AND employee_level IS NOT NULL

),

whole_person_subdimension AS (

  SELECT * FROM {{ref('dim_whole_person_subdimension')}}

)


SELECT
  {{ dbt_utils.surrogate_key(['s.whole_person_subdimension_key','b.industry','b.employee_level']) }} as primary_key,
  s.whole_person_subdimension_key,
  b.industry,
  b.employee_level,
  b.scale_score_mean AS benchmark_scale_score_mean
FROM whole_person_subdimension AS s
-- join benchmarks for each subdimension in dim_whole_person_subdimension
INNER JOIN whole_person_benchmarks AS b
  ON s.whole_person_subdimension_key = b.whole_person_subdimension_key
