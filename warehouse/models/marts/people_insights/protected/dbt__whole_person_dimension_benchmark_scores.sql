WITH construct_benchmark_scores AS (

  SELECT * FROM {{ ref('dbt__whole_person_subdimension_benchmark_scores') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}
  WHERE category_key = 'behavior'

),

avg_calculation as (
SELECT
  s.dimension_key,
  b.whole_person_model_version,
  b.industry,
  b.employee_level,
  AVG(b.scale_score_mean) AS scale_score_mean
FROM construct_benchmark_scores AS b
INNER JOIN whole_person_subdimensions AS s
  ON b.construct_key = s.subdimension_key
GROUP BY
  s.dimension_key,
  b.whole_person_model_version,
  b.industry,
  b.employee_level
),

final as (
  select 
  {{dbt_utils.surrogate_key(['dimension_key', 'whole_person_model_version', 'industry', 'employee_level']) }} as _unique,
    *
  from avg_calculation
)

select * from final