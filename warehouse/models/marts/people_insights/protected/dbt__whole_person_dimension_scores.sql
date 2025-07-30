WITH behavior_subdimension_scores AS (

  SELECT * FROM {{ ref('dbt__whole_person_subdimension_scores') }}
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
  GROUP BY dimension_key

), 
grouped as (
  SELECT
    s.assessment_id,
    s.dimension_key,
    s.whole_person_model_version,
    s.domain_key,
    s.category_key,
    AVG(s.raw_score) AS raw_score,
    AVG(s.original_scale_score) AS original_scale_score,
    AVG(s.scale_score) AS scale_score
  FROM behavior_subdimension_scores AS s
  INNER JOIN whole_person_dimensions AS d
    ON s.dimension_key = d.dimension_key
  GROUP BY
    s.assessment_id,
    s.dimension_key,
    s.whole_person_model_version,
    s.domain_key,
    s.category_key,
    d.subdimension_count
  -- filter out any results that don't have the correct number of subdimension scores
  HAVING
    COUNT(*) = d.subdimension_count
),
final as (
  select {{ dbt_utils.surrogate_key(['assessment_id','dimension_key'])}} as whole_person_dimension_score_id,
    assessment_id,
    dimension_key,
    whole_person_model_version,
    domain_key,
    category_key,
    raw_score,
    original_scale_score,
    scale_score
  from grouped
)
select * from final
