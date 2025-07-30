WITH whole_person_subdimension_scores AS (

  SELECT * FROM {{ref('dbt__whole_person_subdimension_scores')}}

),

whole_person_dimension_scores AS (

  SELECT * FROM {{ref('dbt__whole_person_dimension_scores')}}

),

derived_construct_scores AS (

  SELECT * FROM {{ref('dbt__derived_construct_scores')}}

),

assessments AS (

  SELECT * FROM {{ ref('int_app__assessments') }}

),

track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}

),

construct_scores AS (

  SELECT
    assessment_id,
    subdimension_key AS construct_key,
    PARSE_JSON('{
      "model": "' || whole_person_model_version || '",
      "construct_type": "subdimension",
      "dimension": "' || dimension_key || '",
      "domain": "' || domain_key || '",
      "category": "' || category_key || '"
      }') AS construct_attributes,
    raw_score,
    construct_reference_population_id,
    construct_reference_population_uuid,
    score_mean,
    score_standard_deviation,
    original_scale_score,
    scale_score,
    'subdimension' AS construct_type
  FROM whole_person_subdimension_scores
  WHERE category_key = 'behavior'

  UNION ALL

  SELECT
    assessment_id,
    subdimension_key AS construct_key,
    PARSE_JSON('{
      "model": "' || whole_person_model_version || '",
      "construct_type": "subdimension",
      "category": "' || category_key || '"
      }') AS construct_attributes,
    raw_score,
    construct_reference_population_id,
    construct_reference_population_uuid,
    score_mean,
    score_standard_deviation,
    original_scale_score,
    scale_score,
    'subdimension' AS construct_type
  FROM whole_person_subdimension_scores
  WHERE category_key IN ('mindset', 'outcome')

  UNION ALL

  SELECT
    assessment_id,
    dimension_key AS construct_key,
    PARSE_JSON('{
      "model": "' || whole_person_model_version || '",
      "construct_type": "dimension",
      "domain": "' || domain_key || '",
      "category": "' || category_key || '"
      }') AS construct_attributes,
    raw_score,
    -- Since dimensions are aggregates of subdimensions, these metrics don't provide any value
    NULL AS construct_reference_population_id,
    NULL AS construct_reference_population_uuid,
    NULL AS score_mean,
    NULL AS score_standard_deviation,
    original_scale_score,
    scale_score,
    'dimension' AS construct_type
  FROM whole_person_dimension_scores

  UNION ALL

  SELECT
    assessment_id,
    construct_key,
    PARSE_JSON('{
      "model": "WPM 2.0",
      "construct_type": "construct",
      "reference_population": "' || IFNULL(reference_population_key, '') || '",
      "reference_population_subgroup": "' || IFNULL(reference_population_subgroup_key, '') || '"
      }') AS construct_attributes,
    raw_score,
    construct_reference_population_id,
    construct_reference_population_uuid,
    score_mean,
    score_standard_deviation,
    -- Same as it's always been
    scale_score AS original_scale_score,
    scale_score,
   'construct' AS construct_type
  FROM derived_construct_scores

)


SELECT
  {{ dbt_utils.surrogate_key(['s.assessment_id', 's.construct_key', 's.construct_type']) }} AS primary_key,
  a.user_id AS member_id,
  a.track_assignment_id,
  ta.track_id,
  s.construct_key,
  s.construct_type,
  s.construct_reference_population_id,
  s.construct_reference_population_uuid,
  s.raw_score,
  s.score_mean AS scale_score_mean,
  s.score_standard_deviation AS scale_score_standard_deviation,
  s.original_scale_score,
  -- Fall back to original score - This should only happen for 'turnover_intentions'
  IFF(s.construct_key = 'turnover_intentions', COALESCE(s.scale_score, s.original_scale_score), s.scale_score) AS scale_score,
  s.construct_attributes,
  s.assessment_id,
  a.type AS assessment_type,
  a.submitted_at,
  a.is_duplicate_assessment
FROM construct_scores AS s
INNER JOIN assessments AS a ON s.assessment_id = a.assessment_id
INNER JOIN track_assignments AS ta ON a.track_assignment_id = ta.track_assignment_id
