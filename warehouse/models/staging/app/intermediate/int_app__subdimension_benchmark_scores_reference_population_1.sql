WITH construct_benchmark_scores_reference_population_1 AS (

  SELECT * FROM {{ ref('stg_app__construct_benchmark_scores_reference_population_1') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

SELECT
  primary_key,
  whole_person_model_version,
  key,
  industry,
  level,
  mean
FROM construct_benchmark_scores_reference_population_1
WHERE key IN (
  SELECT subdimension_key FROM whole_person_subdimensions
)