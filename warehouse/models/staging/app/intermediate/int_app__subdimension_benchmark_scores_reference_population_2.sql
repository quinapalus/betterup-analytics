WITH construct_benchmark_scores_reference_population_2 AS (

  SELECT * FROM {{ ref('stg_app__construct_benchmark_scores_reference_population_2') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

SELECT
  primary_key,
  reference_population_id,
  reference_population_key,
  benchmark_population_type,
  country,
  industry,
  level,
  key,
  mean
FROM construct_benchmark_scores_reference_population_2
WHERE key IN (
  SELECT subdimension_key FROM whole_person_subdimensions
)