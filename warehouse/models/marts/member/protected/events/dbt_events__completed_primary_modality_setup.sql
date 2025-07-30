WITH users AS (

  SELECT * FROM  {{ ref('int_app__users') }}

)


SELECT
  user_id AS member_id,
  completed_primary_modality_setup_at AS event_at,
  'completed' AS event_action,
  'primary_modality_setup' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'User' AS associated_record_type,
  user_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM users