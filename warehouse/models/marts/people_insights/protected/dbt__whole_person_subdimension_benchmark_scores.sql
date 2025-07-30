WITH construct_benchmark_scores AS (

  SELECT * FROM {{ ref('stg_app__construct_benchmark_scores') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

SELECT
  *
FROM construct_benchmark_scores
WHERE construct_key IN (
  SELECT subdimension_key FROM whole_person_subdimensions
)
