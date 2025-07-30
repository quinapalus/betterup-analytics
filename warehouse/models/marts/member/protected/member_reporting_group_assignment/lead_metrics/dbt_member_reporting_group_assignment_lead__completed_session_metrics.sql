WITH appointments AS (

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

billable_events__completed_sessions_distinct AS (

  SELECT * FROM {{ ref('stg_app__billable_events') }}
  WHERE event_type = 'completed_sessions'
    -- Note that associated_record_type is aliased from Appointment to
    -- Session in legacy app_billable_events base model
    AND associated_record_type = 'Session'
  -- select the most recent billable event if there are multiple,
  -- this protects against a handful of edge cases, all from 2017
  QUALIFY ROW_NUMBER() OVER (PARTITION BY associated_record_id ORDER BY event_at DESC) = 1

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
    a.complete_at AS event_at,
    'Appointment' AS associated_record_type,
    a.appointment_id AS associated_record_id,
    '{{ appointment_type }}_appointment' AS event_object,
    CASE
      WHEN '{{ appointment_type }}' = 'secondary'
      THEN OBJECT_CONSTRUCT('extended_network_topic_key', sv.key,
                            'extended_network_topic', sv.name,
                            'event_at', be.event_at,
                            'session_minutes', COALESCE(be.usage_minutes, a.requested_length))
      ELSE OBJECT_CONSTRUCT('event_at', be.event_at,
                            'session_minutes', COALESCE(be.usage_minutes, a.requested_length))
    END AS attributes
  FROM appointments AS a
  INNER JOIN coach_assignments AS c
    ON a.coach_assignment_id = c.coach_assignment_id
  LEFT JOIN specialist_verticals AS sv
    ON c.specialist_vertical_uuid = sv.specialist_vertical_uuid
  LEFT JOIN billable_events__completed_sessions_distinct AS be
    ON a.appointment_id = be.associated_record_id
  WHERE c.role = '{{ appointment_type }}'
  AND a.complete_at IS NOT NULL

),

{%- endfor -%}


unioned AS (

{%- for appointment_type in appointment_types %}

  SELECT * FROM {{ appointment_type }}_appointment
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor %}


),

dbt_events__completed_appointment AS (

    SELECT
  member_id,
  event_at,
  'completed' AS event_action,
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

events__completed_lead_appointment AS (

  SELECT
    member_id,
    event_object,
    attributes:"event_at"::timestamp_ntz AS event_at,
    attributes:"session_minutes"::numeric / 60.0 AS session_hours
  FROM dbt_events__completed_appointment
  WHERE event_object IN ('primary_appointment',
                         'secondary_appointment',
                         'on_demand_appointment')

),

final as (

  SELECT
    {{ dbt_utils.surrogate_key(['rga.member_id', 'rga.reporting_group_id', 'rga.associated_assignment_id']) }} as member_reporting_group_assignment_key,    
    rga.member_id,
    rga.reporting_group_id,
    rga.associated_assignment_id,
    MIN(ca.event_at) AS first_session_at,
    MIN(CASE WHEN ca.event_object = 'primary_appointment'
          THEN ca.event_at END) AS first_primary_session_at,
    MAX(ca.event_at) AS last_session_at,
    MAX(CASE WHEN ca.event_object = 'primary_appointment'
                THEN ca.event_at END) AS last_primary_session_at,
    COUNT(*) AS completed_session_count,
    SUM(ca.session_hours) AS completed_session_hours,
    AVG(ca.session_hours) AS average_session_length_hours,
    SUM(IFF(ca.event_object = 'primary_appointment', 1, 0))
      AS completed_primary_session_count,
    SUM(IFF(ca.event_object = 'secondary_appointment', 1, 0))
      AS completed_extended_network_session_count,
    SUM(IFF(ca.event_object = 'on_demand_appointment', 1, 0))
      AS completed_on_demand_session_count
  FROM reporting_group_assignments AS rga
  INNER JOIN events__completed_lead_appointment AS ca
      ON rga.member_id = ca.member_id AND
        ca.event_at >= rga.starts_at AND
        (rga.ended_at IS NULL OR ca.event_at < rga.ended_at)
  GROUP BY member_reporting_group_assignment_key,  rga.member_id, rga.reporting_group_id, rga.associated_assignment_id

)

select * from final
