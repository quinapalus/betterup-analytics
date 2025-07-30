WITH engagement_events AS (

  SELECT * FROM {{ ref('stg_app__engagement_events') }}

),


final AS (

  SELECT DISTINCT
    user_id AS member_id,
    event_at,
    verb AS event_action,
    'group_message' AS event_object,
    event_action || ' ' || event_object AS event_action_and_object,
    eventable_type AS associated_record_type,
    eventable_id AS associated_record_id,
    OBJECT_CONSTRUCT() AS attributes
  FROM engagement_events
  WHERE eventable_type = 'GroupCoachingCohort'
    AND verb = 'sent'
    AND eventable_subject = 'message'

)

SELECT 
  {{dbt_utils.surrogate_key(['member_id', 'event_at', 'event_object', 'associated_record_id']) }} as _unique,
  * 
FROM final 