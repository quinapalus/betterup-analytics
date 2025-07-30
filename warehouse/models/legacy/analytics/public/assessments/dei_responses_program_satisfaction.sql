WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

programsat_meta AS (

  SELECT * FROM {{ref('item_definition_program_satisfaction')}}

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
  pm.item_prompt,
  pm.scale
FROM assessment_items AS ai
INNER JOIN programsat_meta AS pm
  ON ai.item_key = pm.item_key AND
     ai.type = 'Assessments::WholePersonProgramCheckinAssessment'
