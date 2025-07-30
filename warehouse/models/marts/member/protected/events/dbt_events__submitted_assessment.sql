WITH assessments AS (

  SELECT *, responses:"role" AS responses_role
  FROM  {{ ref('stg_app__assessments') }}

)


SELECT DISTINCT
  user_id AS member_id,
  submitted_at AS event_at,
  'submitted' AS event_action,
  'assessment' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'Assessment' AS associated_record_type,
  assessment_id AS associated_record_id,
  OBJECT_CONSTRUCT('assessment_type', type,
      'track_assignment_id', track_assignment_id,
      'responses_role', responses_role,
      'parent_id', parent_id) AS attributes
FROM assessments
WHERE submitted_at IS NOT NULL
