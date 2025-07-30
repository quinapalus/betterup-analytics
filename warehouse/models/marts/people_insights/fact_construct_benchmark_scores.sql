{{
  config(
    tags=["eu"]
  )
}}

WITH whole_person_subdimension_benchmark_scores AS (

  SELECT * FROM {{ref('dbt__whole_person_subdimension_benchmark_scores')}}

),

whole_person_dimension_benchmark_scores AS (

  SELECT * FROM {{ref('dbt__whole_person_dimension_benchmark_scores')}}

),

derived_construct_benchmark_scores AS (

  SELECT * FROM {{ref('dbt__derived_construct_benchmark_scores')}}

),

construct_benchmark_scores AS (

  SELECT
    'subdimension' as construct_type,
    whole_person_model_version,
    industry,
    employee_level,
    construct_key,
    scale_score_mean AS scale_score
  FROM whole_person_subdimension_benchmark_scores

  UNION ALL

  SELECT
    'dimension' as construct_type,
    whole_person_model_version,
    industry,
    employee_level,
    dimension_key AS construct_key,
    scale_score_mean AS scale_score
  FROM whole_person_dimension_benchmark_scores

  UNION ALL

  SELECT
    'construct' as construct_type,
    whole_person_model_version,
    industry,
    employee_level,
    construct_key,
    scale_score_mean AS scale_score
  FROM derived_construct_benchmark_scores

)


SELECT
  {{ dbt_utils.surrogate_key(['construct_type', 'construct_key', 'industry', 'employee_level']) }} AS primary_key,
  *
FROM construct_benchmark_scores
