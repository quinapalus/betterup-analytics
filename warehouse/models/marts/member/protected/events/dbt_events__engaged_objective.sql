WITH engagement_events AS (

  SELECT * FROM  {{ ref('stg_app__engagement_events') }}

)


SELECT DISTINCT
  -- Surrogate Primary Key of MEMBER_ID, EVENT_ACTION_AND_OBJECT, EVENT_AT, ASSOCIATED_RECORD_ID
  {{ dbt_utils.surrogate_key(['user_id', 'verb', 'event_at', 'eventable_id']) }} AS events__engaged_objective_id,
  user_id AS member_id,
  event_at,
  verb AS event_action,
  CASE
    WHEN eventable_type = 'ObjectiveCheckIn' THEN 'objective_check_in'
    ELSE 'objective'
  END AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  eventable_type AS associated_record_type,
  eventable_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM engagement_events
WHERE eventable_type IN ('Objective', 'ObjectiveCheckIn')
AND verb IN ('completed', 'created', 'edited', 'shared')