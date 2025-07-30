WITH  appointments AS (

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
  'on_demand'
  ]
  -%}


{%- for appointment_type in appointment_types -%}

{{appointment_type}}_appointment AS (

  SELECT DISTINCT
    a.member_id,
    a.starts_at AS event_at,
   'Appointment' AS associated_record_type,
    a.appointment_id AS associated_record_id,
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
  FROM appointments AS a
  INNER JOIN coach_assignments AS c
    ON a.coach_assignment_id = c.coach_assignment_id
  LEFT JOIN specialist_verticals AS sv
    ON c.specialist_vertical_uuid = sv.specialist_vertical_uuid
  WHERE c.role = '{{ appointment_type }}'
  AND a.started_at IS NULL
  AND NOT a.is_appointment_missed
  AND a.canceled_at IS NULL
  AND a.complete_at IS NULL

),

{%- endfor -%}



unioned AS (

{%- for appointment_type in appointment_types %}

  SELECT * FROM {{ appointment_type }}_appointment
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor %}


),

dbt_events__scheduled_appointment AS (

    SELECT
  member_id,
  event_at,
  'scheduled' AS event_action,
  event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  associated_record_type,
  associated_record_id,
  attributes
FROM unioned


),


reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}

),

events__scheduled_lead_appointment AS (

  SELECT
    member_id,
    event_object,
    event_at,
    attributes:"starts_at"::timestamp AS starts_at
  FROM dbt_events__scheduled_appointment
  WHERE event_object IN ('primary_appointment',
                         'secondary_appointment',
                         'on_demand_appointment') AND
        starts_at > CURRENT_TIMESTAMP()
)


SELECT
  {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id']) }} AS primary_key,
  rga.member_id,
  rga.reporting_group_id,
  MIN(sa.starts_at) AS next_session_at,
  MIN(CASE WHEN sa.event_object = 'primary_appointment'
               THEN sa.starts_at END) AS next_primary_session_at,
  COUNT(*) AS scheduled_upcoming_session_count,
  SUM(IFF(sa.event_object = 'primary_appointment', 1, 0))
    AS scheduled_upcoming_primary_session_count,
  SUM(IFF(sa.event_object = 'secondary_appointment', 1, 0))
    AS scheduled_upcoming_extended_network_session_count,
  SUM(IFF(sa.event_object = 'on_demand_appointment', 1, 0))
    AS scheduled_upcoming_on_demand_session_count
FROM reporting_group_assignments AS rga
INNER JOIN events__scheduled_lead_appointment AS sa
    ON rga.member_id = sa.member_id AND
       sa.event_at >= rga.starts_at AND
       (rga.ended_at IS NULL OR sa.event_at < rga.ended_at)
GROUP BY rga.member_id, rga.reporting_group_id
