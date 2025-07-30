WITH user_title_updated AS (

  SELECT * FROM {{ ref('stg_segment_backend__user_title_updated') }}

)


SELECT DISTINCT
  user_id AS member_id,
  timestamp AS event_at,
  'completed' AS event_action,
  'profile' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'User' AS associated_record_type,
  user_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM user_title_updated