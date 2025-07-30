WITH persisted_construct_scores AS (

    SELECT * FROM {{ ref('dbt__persisted_construct_scores') }}

),

whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

SELECT
  ps.primary_key,
  ps.assessment_id,
  ps.construct_id,
  ps.key as subdimension_key,
  ps.raw_score,
  ps.construct_reference_population_id,
  ps.construct_reference_population_uuid,
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
  d.dimension_key,
  d.domain_key,
  d.category_key
FROM persisted_construct_scores AS ps
INNER JOIN whole_person_subdimensions AS d ON ps.key = d.subdimension_key
WHERE ps.assessment_type IN ('Assessments::WholePersonAssessment',
               'Assessments::WholePersonProgramCheckinAssessment',
               'Assessments::WholePersonGroupCoachingCheckinAssessment')
    AND ps.questions_version = '2.0'
    -- extract subdimension scores
    AND ps.type = 'Scores::SubdimensionScore'