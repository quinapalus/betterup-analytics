-- This model moves derived table logic from Looker ito dbt for performance
-- This model should be refactored / remodeled to be more performant and all aggregate functions should be reviewed for accuracy
{{
  config(
    tags=["eu"]
  )
}}

WITH member_sessions AS (

  SELECT * FROM {{ref('stg_app__appointments')}}

),
member_track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),
member_tracks AS (

  SELECT * FROM {{ref('stg_app__tracks')}}

),
coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

),
specialist_verticals AS (

  SELECT * FROM {{ref('stg_curriculum__specialist_verticals')}}

),
billable_events AS (

  SELECT * FROM {{ref('stg_app__billable_events')}}

),
completed_sessions_billable_events AS (
SELECT
  associated_record_id,
  SUM(CASE WHEN event_type = 'completed_sessions' THEN 1 ELSE 0 END) as completed_sessions_count,
  SUM(CASE WHEN event_type = 'completed_sessions' THEN usage_minutes ELSE 0 END) as completed_session_usage_minutes
  FROM billable_events
  WHERE associated_record_type = 'Session'
  GROUP BY 1
),
appointment_creates AS (
  SELECT
  version_id,
  item_id AS appointment_id,
  COALESCE(TO_BOOLEAN(GET_PATH(object_changes, 'recurring[1]')), FALSE) AS was_recurring
  FROM {{ ref ('stg_app__versions_create') }}
  WHERE item_type = 'Appointment'
),

final as (

  SELECT
    s.appointment_id as session_id,
    s.member_id,
    s.coach_id,
    ta.track_id,
    s.appointment_created_at AS scheduled_at,
    s.appointment_updated_at,
    s.requested_length,
    s.is_appointment_recurring,
    s.starts_at,
    s.started_at,
    s.ends_at,
    s.canceled_at,
    s.complete_at,
    s.is_appointment_missed,
    s.sequence_number,
    s.coach_assignment_id,
    s.track_assignment_id,
    ca.role AS coach_type,
    sv.name AS extended_network_session_type,
    s.call_id,
    s.development_topic_id,
    s.post_session_member_assessment_id,
    csbe.completed_sessions_count > 0 AS completed_session_billable_event,
    completed_session_usage_minutes,

    row_number() over (partition by s.member_id order by s.appointment_created_at) as session_number_per_user,

    --timestamp of when the next session in coaching assignment was created. 
    lead(scheduled_at,1) over(
      partition by s.coach_assignment_id
      order by s.complete_at,s.ends_at
    ) as next_session_scheduled_at,

    /*
    Hours between when session ended and when the next session in the coaching assignment was scheduled.
    For example - If I finish a session today at 4PM and the next session in the coaching assignment is then scheduled (created) today at 6PM 
    the calculation should return 2 hours for the session that finished today at 4PM.
    */ 
    
    datediff(hour, s.complete_at , next_session_scheduled_at) as hours_between_session_completed_and_next_session_scheduled,

    sum(csbe.completed_sessions_count) over (partition by s.member_id) as completed_sessions_per_user, --probably isn't giving the right count
    ac.was_recurring,
    CASE WHEN s.creator_id = s.coach_id THEN 'coach' WHEN s.creator_id = s.member_id THEN 'member' ELSE 'other' END AS creator_role
  FROM member_sessions AS s
  INNER JOIN member_track_assignments AS ta
    ON s.track_assignment_id = ta.track_assignment_id
  LEFT JOIN member_tracks AS t
    ON ta.track_id = t.track_id
  LEFT JOIN coach_assignments AS ca
    ON s.coach_assignment_id = ca.coach_assignment_id
  LEFT OUTER JOIN specialist_verticals AS sv
    ON ca.specialist_vertical_id = sv.specialist_vertical_id
  LEFT OUTER JOIN completed_sessions_billable_events AS csbe
    ON csbe.associated_record_id = s.appointment_id
  LEFT OUTER JOIN appointment_creates AS ac
    ON ac.appointment_id = s.appointment_id)

select
  final.*,
  --role of the user who scheduled next session in coaching assignment. Can be member or coach
  lead(creator_role,1) over(
    partition by coach_assignment_id
    order by complete_at,ends_at
  ) as next_session_scheduled_by_role
from final
