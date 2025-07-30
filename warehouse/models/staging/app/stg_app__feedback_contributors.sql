WITH feedback_contributors AS (

  SELECT * FROM {{ source('app', 'feedback_contributors') }}

)

SELECT
  id as feedback_contributor_id,
  feedback_request_id, 
  user_id,
  contributor_assessment_id,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM feedback_contributors
