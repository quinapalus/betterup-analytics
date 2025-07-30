WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

coachmatch_meta AS (

  SELECT * FROM {{ref('item_definition_coach_matching')}}

)

SELECT
  ai.type AS source,
  ai.assessment_id,
  ai.created_at,
  ai.item_key,
  CASE WHEN is_integer(ai.item_response::variant) = true THEN ai.item_response::int ELSE NULL END AS item_response,
  CASE WHEN is_integer(ai.item_response::variant) = false THEN ai.item_response ELSE NULL END AS item_response_text,
  ai.sequence,  -- Sequence iterates within each source
  ai.submitted_at,
  ai.user_id AS member_id,
  cm.subsurvey,
  cm.item_prompt,
  cm.scale
FROM assessment_items AS ai
INNER JOIN coachmatch_meta AS cm
  ON ai.item_key = cm.item_key AND
     ai.type = 'Assessments::CoachMatchingAssessment'
