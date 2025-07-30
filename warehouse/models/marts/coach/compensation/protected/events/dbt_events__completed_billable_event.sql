WITH billable_events AS (
    SELECT * FROM {{ ref('stg_app__billable_events') }}
),

track_assignments AS (
  SELECT * FROM {{ ref('stg_app__track_assignments') }}
),

tracks AS (
  SELECT * FROM {{ ref('dim_tracks') }}
),

appointments AS (
  SELECT * FROM {{ ref('stg_app__appointments') }}
),

coach_assignments AS (
  SELECT * FROM {{ ref('stg_app__coach_assignments') }}
),

billable_event_details AS (

    SELECT 
    be.billable_event_id,
    be.coach_id,
    be.member_id,
    be.event_at,
    ca.specialist_vertical_uuid,
    ca.specialist_vertical_id,
    t.deployment_group,
    t.deployment_type,
    be.coaching_cloud,
    ca.role AS coaching_assignment_role,
    be.event_type AS billing_event_type,
    be.associated_record_type AS billable_event_associated_record_type,
    be.associated_record_id AS billable_event_associated_record_id,
    be.units AS billable_event_hours,
    be.amount_due_usd,
    t.minutes_limit,
    case 
      when be.coaching_cloud = 'professional' 
      then 1 
      else 0 end as is_professional_coaching_cloud,
    case
      when t.deployment_group in ('B2B / Gov Paid Contract','BetterUp')
           and ca.role = 'primary'
           and be.coaching_cloud = 'professional'
      then true 
      else false end as is_primary_b2b_flag
      

    FROM billable_events be 
    LEFT JOIN appointments AS a ON a.appointment_id = be.ASSOCIATED_RECORD_ID
    LEFT JOIN coach_assignments AS ca ON ca.coach_assignment_id = a.COACH_ASSIGNMENT_ID 
    LEFT JOIN track_assignments AS ta ON ta.track_assignment_id = a.TRACK_ASSIGNMENT_ID 
    LEFT JOIN tracks AS t ON t.track_id = ta.track_id 
)

SELECT 
    coach_id, 
    member_id,
    event_at,
    'completed' AS event_action,
    'billable event' AS event_object,
    event_action || ' ' || event_object AS event_action_and_object,
    'Billable Event' AS associated_record_type,
    billable_event_id AS associated_record_id,
    object_construct('deployment_group',deployment_group,
                      'deployment_type',deployment_type,
                      'coaching_assignment_role',coaching_assignment_role,
                      'billing_event_type',billing_event_type,
                      'billable_event_associated_record_id',billable_event_associated_record_id,
                      'billable_event_associated_record_type',billable_event_associated_record_type,
                      'billable_event_hours',billable_event_hours,
                      'amount_due_usd',amount_due_usd,
                      'is_primary_b2b_flag',is_primary_b2b_flag,
                      'is_professional_coaching_cloud',is_professional_coaching_cloud,
                      'specialist_vertical_uuid',specialist_vertical_uuid,
                      'specialist_vertical_id',specialist_vertical_id,
                      'minutes_limit',minutes_limit
                     ) AS attributes --we use these attributes to filter and create additional measures in downstream models and looker
FROM billable_event_details

