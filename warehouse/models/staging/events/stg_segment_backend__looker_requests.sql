WITH looker_requests AS (

  SELECT * FROM {{ source('segment_backend', 'looker_request') }}

)

SELECT
  id AS event_id,
  user_id,
  anonymous_id,
  received_at,
  event_text,
  create_resp_duration,
  run_query_duration,
  total_duration,
  status_code,
  query_id,
  request_group_id
FROM looker_requests