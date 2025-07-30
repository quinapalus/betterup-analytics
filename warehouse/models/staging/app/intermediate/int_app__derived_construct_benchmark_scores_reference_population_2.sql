with construct_benchmark_scores_reference_population_2 as (

    select * from {{ ref('stg_app__construct_benchmark_scores_reference_population_2') }}

),

constructs AS (

  SELECT * FROM {{ ref('stg_assessment__constructs') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

SELECT
    *
FROM construct_benchmark_scores_reference_population_2
WHERE key IN (
  SELECT construct_key FROM constructs
)
AND key NOT IN (
  SELECT subdimension_key FROM whole_person_subdimensions
)
