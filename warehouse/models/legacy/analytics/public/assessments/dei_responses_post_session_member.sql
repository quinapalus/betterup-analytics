WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

postsesh_meta AS (

  SELECT * FROM {{ref('item_definition_post_session_member')}}

),

sessions AS (

  SELECT * FROM {{ref('stg_app__sessions')}}

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

assessment_session_tracks AS (

  SELECT
    ai.assessment_id,
    ai.item_response::int AS session_id,
    ta.track_id
  FROM assessment_items AS ai
  INNER JOIN sessions AS s ON ai.item_response::int = s.session_id
  INNER JOIN track_assignments AS ta ON s.track_assignment_id = ta.track_assignment_id
  WHERE ai.type = 'Assessments::PostSessionMemberAssessment' AND
        ai.item_key = 'appointment_id'

)


SELECT
  ai.type AS source,
  ai.assessment_id,
  ast.session_id,
  ast.track_id,
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
INNER JOIN postsesh_meta AS pm
  ON ai.type = 'Assessments::PostSessionMemberAssessment' AND
     ai.item_key = pm.item_key
LEFT OUTER JOIN assessment_session_tracks AS ast
  ON ai.assessment_id = ast.assessment_id
