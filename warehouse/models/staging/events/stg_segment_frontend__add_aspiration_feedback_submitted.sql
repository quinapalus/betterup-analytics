WITH add_aspiration_feedback_submitted AS (

  SELECT * FROM {{ source('segment_frontend', 'add_aspiration_feedback_submitted') }}

)

SELECT
  id,
  user_id,
  page_name,
  page_url,
  context_page_title,
  context_page_url,
  platform,
  desired_aspiration_count,
  event,
  to_timestamp(timestamp) AS event_at
FROM add_aspiration_feedback_submitted