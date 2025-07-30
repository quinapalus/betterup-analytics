WITH engagement_events AS (

  SELECT * FROM {{ ref('stg_app__engagement_events') }}

)


SELECT
  -- Surrogate Key of Member ID, Event At, Associated Record Type, Associated Record ID. Functions as a composite primary key.
  {{ dbt_utils.surrogate_key(['user_id', 'event_at', 'eventable_type', 'eventable_id']) }} AS primary_key,
  user_id AS member_id,
  event_at,
  'selected' AS event_action,
  'focus_area' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  eventable_type AS associated_record_type,
  eventable_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM engagement_events
WHERE eventable_type = 'GrowthMap'
AND verb = 'created'