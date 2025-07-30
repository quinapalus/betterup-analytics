WITH engagement_events AS (

  SELECT * FROM  {{ ref('stg_app__engagement_events') }}

)


SELECT DISTINCT
  user_id AS member_id,
  event_at,
  CASE
    WHEN eventable_type = 'RatedResource' THEN 'rated'
    ELSE verb
  END AS event_action,
  'resource' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  eventable_type AS associated_record_type,
  eventable_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM engagement_events
WHERE (eventable_type = 'Resource' AND verb IN ('shared', 'viewed'))
OR (eventable_type = 'RatedResource' AND verb = 'created')