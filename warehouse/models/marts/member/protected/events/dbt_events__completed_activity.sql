WITH engagement_events AS (

  SELECT * FROM  {{ ref('stg_app__engagement_events') }}

), activities AS (

  SELECT * FROM  {{ ref('stg_app__activities') }}

), resources AS (

  SELECT * FROM  {{ ref('stg_app__resources') }}

)

SELECT DISTINCT
  ae.user_id AS member_id,
  ae.event_at,
  ae.verb AS event_action,
  'activity' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  ae.eventable_type AS associated_record_type,
  ae.eventable_id AS associated_record_id,
  OBJECT_CONSTRUCT(
      'duration', r.duration,
      'resource_id', r.resource_id,
      'completed_at', a.completed_at
      ) AS attributes
FROM engagement_events AS ae
LEFT JOIN activities AS a
    ON ae.eventable_id = a.activity_id
LEFT JOIN resources AS r
    ON a.resource_id = r.resource_id
WHERE ae.eventable_type = 'Activity'
AND ae.verb = 'completed'
