WITH engagement_events AS (

  SELECT * FROM {{ ref('stg_app__engagement_events') }}

),

appointments AS (

  SELECT * FROM {{ ref('stg_app__appointments') }}

),

group_coaching_appointments AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_appointments') }}

),

coach_assignments AS (

  SELECT * FROM {{ ref('stg_app__coach_assignments') }}

),

specialist_verticals AS (

  SELECT * FROM {{ ref('stg_curriculum__specialist_verticals') }}

),


{%- set appointment_types = [
  'primary',
  'secondary',
  'on_demand',
  'care',
  'peer'
  ]
  -%}


{%- for appointment_type in appointment_types -%}

{{appointment_type}}_appointment AS (

  SELECT DISTINCT
    e.user_id AS member_id,
    e.event_at,
    e.eventable_type AS associated_record_type,
    e.eventable_id AS associated_record_id,
    '{{ appointment_type }}_appointment' AS event_object,
    CASE
      WHEN '{{ appointment_type }}' = 'secondary'
      THEN OBJECT_CONSTRUCT('extended_network_topic_key', sv.key,
                            'extended_network_topic', sv.name,
                            'coach_assignment_date', c.created_at,
                            'starts_at', a.starts_at,
                            'requested_length', a.requested_length)
      ELSE OBJECT_CONSTRUCT('coach_assignment_date', c.created_at,
                            'starts_at', a.starts_at,
                            'requested_length', a.requested_length)
    END AS attributes
  FROM engagement_events AS e
  INNER JOIN appointments AS a
    ON e.eventable_id = a.appointment_id
  INNER JOIN coach_assignments AS c
    ON a.coach_assignment_id = c.coach_assignment_id
  LEFT JOIN specialist_verticals AS sv
    ON c.specialist_vertical_uuid = sv.specialist_vertical_uuid
  WHERE c.role = '{{ appointment_type }}'
  AND e.eventable_type = 'Appointment'
  AND e.verb = 'scheduled'

),

{%- endfor -%}

group_coaching_appointment AS (

  SELECT
    member_id,
    created_at AS event_at,
    'GroupCoachingAppointment' AS associated_record_type,
    group_coaching_appointment_id AS associated_record_id,
    'group_coaching_appointment' AS event_object,
    OBJECT_CONSTRUCT() AS attributes
  FROM group_coaching_appointments

),

unioned AS (

{%- for appointment_type in appointment_types %}

  SELECT * FROM {{ appointment_type }}_appointment UNION ALL

{%- endfor %}

  SELECT * FROM group_coaching_appointment

)


SELECT
  {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'event_at', 'associated_record_id']) }} AS dbt_events__scheduled_appointment_id,
  member_id,
  event_at,
  'scheduled' AS event_action,
  event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  associated_record_type,
  associated_record_id,
  attributes
FROM unioned
