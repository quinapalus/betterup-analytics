WITH assessment_completed AS (

  SELECT * FROM {{ source('segment_backend', 'assessment_completed') }}

)

SELECT
  id,
  assessment_id,
  assessment_parent_id,
  assessment_questions_version,
  event,
  assessment_type,
  user_id,
  track_assignment_id,
  product_subscription_assignment_id,
  assessment_user_id,
  context_library_name,
  to_timestamp(timestamp) AS event_at
FROM assessment_completed