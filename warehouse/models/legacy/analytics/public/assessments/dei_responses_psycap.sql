WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

psycap_meta AS (

  SELECT * FROM {{ref('item_definition_psycap')}}

)

SELECT
  ai.type AS source,
  ai.assessment_id,
  ai.created_at,
  ai.item_key,
  ai.item_response::int as item_response,  -- Assuming psycap response is only an integer
  ai.sequence,  -- Sequence iterates within each source
  ai.submitted_at,
  ai.user_id AS member_id,
  pc.item_prompt,
  pc.scale,
  pc.subdimension
FROM assessment_items AS ai
INNER JOIN psycap_meta AS pc
  ON ai.item_key = pc.item_key AND
     ai.type IN ('Assessments::OnboardingAssessment', 'Assessments::WholePersonProgramCheckinAssessment')
