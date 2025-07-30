WITH assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

)


SELECT

  -- select the first instance of completed assessment for
  -- appointments with multiple associated assessments.
  responses:appointment_id::INT AS session_id,
  assessment_id,
  user_id AS member_id,
  responses,
  created_at,
  submitted_at
FROM assessments
WHERE type = 'Assessments::PostSessionMemberAssessment'
  AND submitted_at IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY responses:appointment_id::INT ORDER BY submitted_at ASC) = 1
