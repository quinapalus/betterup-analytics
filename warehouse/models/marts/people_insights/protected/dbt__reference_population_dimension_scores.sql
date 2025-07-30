WITH reference_population_behavior_subdimension_scores AS (

  SELECT * FROM {{ ref('dbt__reference_population_subdimension_scores') }}
  WHERE category_key = 'behavior'

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

),

whole_person_dimensions AS (

  SELECT
    dimension_key,
    COUNT(*) AS subdimension_count
  FROM whole_person_subdimensions
  WHERE category_key = 'behavior'
  GROUP BY
    dimension_key
)


SELECT
  {{ dbt_utils.surrogate_key(['s.reference_population_id','s.reference_population_subgroup_id',
                              's.assessment_id','s.dimension_key','s.dimension','s.whole_person_model_version',
                              's.domain_key','s.domain','s.category_key','category']) }} as primary_key,
  s.reference_population_id,
  s.reference_population_subgroup_id,
  s.assessment_id,
  s.dimension_key,
  s.dimension,
  s.whole_person_model_version,
  s.domain_key,
  s.domain,
  s.category_key,
  s.category,
  AVG(s.raw_score) AS raw_score,
  AVG(s.original_scale_score) AS original_scale_score,
  AVG(s.scale_score) AS scale_score
FROM reference_population_behavior_subdimension_scores AS s
INNER JOIN whole_person_dimensions AS d
  ON s.dimension_key = d.dimension_key
GROUP BY
  s.reference_population_id,
  s.reference_population_subgroup_id,
  s.assessment_id,
  s.dimension_key,
  s.dimension,
  s.whole_person_model_version,
  s.domain_key,
  s.domain,
  s.category_key,
  s.category,
  d.subdimension_count
-- filter out any results that don't have the correct number of subdimension scores
HAVING
  COUNT(*) = d.subdimension_count
