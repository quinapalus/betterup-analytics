WITH reference_population_persisted_construct_scores AS (

    SELECT * FROM {{ ref('dbt__reference_population_persisted_construct_scores') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('dbt__whole_person_subdimensions') }}

)

SELECT
  {{ dbt_utils.surrogate_key(['ps.assessment_id', 'ps.construct_id', 'ps.reference_population_id', 'ps.reference_population_subgroup_id', 'ps.construct_reference_population_uuid']) }} as primary_key,
  ps.assessment_id,
  ps.construct_id,
  ps.key as subdimension_key,
  ps.name,
  ps.label_i18n,
  ps.raw_score,
  ps.construct_reference_population_id,
  ps.construct_reference_population_uuid,
  ps.reference_population_subgroup_id,
  ps.reference_population_id,
  ps.score_mean,
  ps.score_standard_deviation,
  ps.scale_min_score,
  ps.scale_max_score,
  ps.scale_mean,
  ps.scale_standard_deviation,
  ps.original_scale_score,
  ps.scale_score,
  -- denormalize in WPM model attributes
  d.whole_person_model_version,
  d.category_key,
  d.category,
  d.dimension_key,
  d.dimension,
  d.domain_key,
  d.domain
FROM reference_population_persisted_construct_scores AS ps
INNER JOIN whole_person_subdimensions AS d ON ps.key = d.subdimension_key
WHERE (ps.questions_version = '2.0' or ps.assessment_type = 'Assessments::CareWholePersonModelAssessment' or ps.assessment_type = 'Assessments::CareWholePersonModelCheckinAssessment')
    -- extract subdimension scores
    AND ps.type = 'Scores::SubdimensionScore'
