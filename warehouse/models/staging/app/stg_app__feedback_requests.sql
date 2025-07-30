WITH feedback_requests AS (

  SELECT * FROM {{ source('app', 'feedback_requests') }}

)

SELECT
  id as feedback_request_id,
  member_id, 
  requesting_assessment_id,
  {{ load_timestamp('report_available_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM feedback_requests
