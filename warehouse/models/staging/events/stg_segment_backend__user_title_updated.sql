WITH user_title_updated AS (

  SELECT * FROM {{ source('segment_backend', 'user_title_updated') }}

)

SELECT
  id,
  user_id,
  {{ load_timestamp('title_updated_at') }},
  {{ load_timestamp('timestamp') }}
FROM user_title_updated