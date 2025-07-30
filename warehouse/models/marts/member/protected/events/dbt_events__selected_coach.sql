WITH coach_assignments AS (

  SELECT * FROM {{ ref('stg_app__coach_assignments') }}

),

{%- set coach_types = [
  'primary',
  'secondary',
  'group',
  'on_demand',
  'care',
  'peer'
  ]
  -%}


{%- for coach_type in coach_types -%}

{{coach_type}}_coach AS (

  SELECT
    member_id,
    created_at AS event_at,
    '{{ coach_type }}_coach' AS event_object,
    coach_assignment_id
  FROM coach_assignments
  WHERE role = '{{ coach_type }}'
  -- include coach selection events for a member's first coach selection, as well as any subsequent
  -- coach selection that follows lapse in a coach assignment of more than 24 hours
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY created_at) = 1 OR
    DATEDIFF(hour, LAG(ended_at) OVER(PARTITION BY member_id ORDER BY created_at), created_at) >= 24

),

{%- endfor -%}

unioned AS (

{%- for coach_type in coach_types -%}

  SELECT * FROM {{ coach_type }}_coach
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}

),

final as (
    SELECT
      member_id,
      event_at,
      'selected' AS event_action,
      event_object,
      event_action || ' ' || event_object AS event_action_and_object,
      'CoachAssignment' AS associated_record_type,
      coach_assignment_id AS associated_record_id,
      OBJECT_CONSTRUCT() AS attributes
    FROM unioned
)

select
    {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'event_at', 'associated_record_id']) }} AS dbt_events__selected_coach_id,
    *
from final