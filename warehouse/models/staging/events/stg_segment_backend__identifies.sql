WITH identifies AS (

  SELECT * FROM {{ source('segment_backend', 'identifies') }}

)

SELECT
  id,
  user_id,
  created_at,
  to_timestamp(timestamp) AS event_at,
  context_library_name,
  is_on_converged_platform,
  foundations_active
FROM identifies