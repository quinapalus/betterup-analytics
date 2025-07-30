WITH assessments AS (

  SELECT * FROM  {{ ref('stg_app__assessments') }}

)


SELECT DISTINCT
  user_id AS member_id,
  created_at AS event_at,
  'started' AS event_action,
  'assessment' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'Assessment' AS associated_record_type,
  assessment_id AS associated_record_id,
  OBJECT_CONSTRUCT('assessment_type', type,
      'track_assignment_id', track_assignment_id , 'submitted_at', submitted_at) AS attributes
FROM assessments
