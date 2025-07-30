WITH reference_population_dimension_scores AS (

  SELECT * FROM {{ ref('dbt__reference_population_dimension_scores') }}
  WHERE category_key = 'behavior'

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('dbt__whole_person_subdimensions') }}

),

whole_person_domains AS (

  SELECT
    domain_key,
    COUNT(distinct dimension_key) AS dimension_count
  FROM whole_person_subdimensions
  WHERE category_key = 'behavior'
  GROUP BY
    domain_key

),

avg_calculation as (
SELECT
  s.reference_population_id,
  s.reference_population_subgroup_id,
  s.assessment_id,
  s.domain_key,
  s.domain,
  s.whole_person_model_version,
  s.category_key,
  s.category,
  AVG(s.raw_score) AS raw_score,
  AVG(s.original_scale_score) AS original_scale_score,
  AVG(s.scale_score) AS scale_score
FROM reference_population_dimension_scores AS s
INNER JOIN whole_person_domains AS d
  ON s.domain_key = d.domain_key
GROUP BY
  s.reference_population_id,
  s.reference_population_subgroup_id,
  s.assessment_id,
  s.domain_key,
  s.domain,
  s.whole_person_model_version,
  s.category_key,
  s.category,
  d.dimension_count
-- filter out any results that don't have the correct number of subdimension scores
HAVING
  COUNT(*) = d.dimension_count
),

final as (
  select
  {{ dbt_utils.surrogate_key(['reference_population_id', 'reference_population_subgroup_id',
                              'assessment_id', 'domain_key', 'domain', 'whole_person_model_version',
                              'category_key', 'category'])}} as _unique,
    *
  from avg_calculation
)

select * from final