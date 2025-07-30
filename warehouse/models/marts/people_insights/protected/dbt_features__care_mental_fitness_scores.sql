WITH scores AS (

  SELECT * FROM {{ ref('dbt__care_well_being_scores') }}

)


SELECT
  scores.user_id AS member_id,
  scores.assessment_id AS associated_record_id,
  'Assessment' AS associated_record_type,
  scores.measurement_date AS feature_collected_at,
  CONCAT('score_', LOWER(REPLACE(scores.construct, '-', '_'))) AS feature_key,
  OBJECT_CONSTRUCT(
    'well_being_cluster', scores.well_being_cluster,
    'construct', scores.construct,
    'scale_score', scores.scale_score
  ) AS classification,
  OBJECT_CONSTRUCT(
    'is_baseline', scores.is_baseline,
    'has_baseline', scores.has_baseline,
    'is_progress', scores.is_progress,
    'has_progress', scores.has_progress,
    'is_most_recent', scores.is_most_recent,
    'next_measurement_at', scores.next_measurement_date
  ) AS feature_attributes,
  'care_mental_fitness_score' AS feature_type
FROM scores
