WITH engagement_events AS (

  SELECT * FROM {{ ref('stg_app__engagement_events') }}

),

final as (
    SELECT
      user_id AS member_id,
      event_at,
      'engaged' AS event_action,
      'user' AS event_object,
      event_action || ' ' || event_object AS event_action_and_object,
      'EngagementEvent' AS associated_record_type,
      engagement_event_id AS associated_record_id,
      OBJECT_CONSTRUCT() AS attributes
    FROM engagement_events
)

select
    {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'event_at', 'associated_record_id']) }} AS dbt_events__user_engaged_id,
    *
from final
