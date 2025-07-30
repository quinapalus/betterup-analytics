WITH engagement_events AS (

  SELECT * FROM  {{ ref('stg_app__engagement_events') }}

),

assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}

),

final as (
    SELECT DISTINCT
      e.user_id AS member_id,
      e.event_at,
      e.verb AS event_action,
      'assessment' AS event_object,
      event_action || ' ' || event_object AS event_action_and_object,
      e.eventable_type AS associated_record_type,
      e.eventable_id AS associated_record_id,
      OBJECT_CONSTRUCT() AS attributes
    FROM engagement_events AS e
    INNER JOIN assessments AS a
      ON e.eventable_id = a.assessment_id AND
         e.user_id = a.user_id
    WHERE e.eventable_type = 'Assessment'
    AND e.verb = 'viewed'
)
select
  {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'event_at',  'associated_record_id']) }} AS dbt_events__viewed_assessment_id,
  *
from final