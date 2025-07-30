WITH construct_benchmark_scores_reference_population_1 AS (

  SELECT * FROM {{ ref('stg_app__construct_benchmark_scores_reference_population_1') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}
  WHERE category_key = 'behavior'

),

aggregated as (
    SELECT
      s.dimension_key,
      b.whole_person_model_version,
      b.industry,
      b.level,
      AVG(b.mean) AS mean
    FROM construct_benchmark_scores_reference_population_1 AS b
    INNER JOIN whole_person_subdimensions AS s
      ON b.key = s.subdimension_key
    GROUP BY
      s.dimension_key,
      b.whole_person_model_version,
      b.industry,
      b.level
)

, final as (

    select
        {{ dbt_utils.surrogate_key(['dimension_key', 'whole_person_model_version', 'industry', 'level']) }} as dimension_benchmark_key,
        dimension_key,
        whole_person_model_version,
        industry,
        level,
        mean
    from aggregated

)

select * from final
