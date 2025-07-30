WITH construct_benchmark_scores_reference_population_2 AS (

  SELECT * FROM {{ ref('stg_app__construct_benchmark_scores_reference_population_2') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}
  WHERE category_key = 'behavior'

),

aggregated as (
    SELECT
        s.dimension_key,
        b.country,
        b.industry,
        b.level,
        b.reference_population_id,
        b.reference_population_key,
        b.benchmark_population_type,
        AVG(b.mean) AS mean
    FROM construct_benchmark_scores_reference_population_2 AS b
    INNER JOIN whole_person_subdimensions AS s
        ON b.key = s.subdimension_key
    GROUP BY
        s.dimension_key,
        b.country,
        b.industry,
        b.level,
        b.reference_population_id,
        b.reference_population_key,
        b.benchmark_population_type
)

select * from aggregated