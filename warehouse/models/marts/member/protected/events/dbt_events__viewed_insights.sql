WITH engagement_events AS (

  SELECT * FROM {{ ref('stg_app__engagement_events') }}

)


SELECT DISTINCT
  {{ dbt_utils.surrogate_key(['user_id', 'eventable_id','event_at']) }} AS member_eventable_id,
  user_id AS member_id,
  event_at,
  verb AS event_action,
  eventable_subject AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  eventable_type AS associated_record_type,
  eventable_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM engagement_events
WHERE eventable_type = 'User'
AND verb = 'viewed'
AND eventable_subject IN ('member_insights_dashboard')
