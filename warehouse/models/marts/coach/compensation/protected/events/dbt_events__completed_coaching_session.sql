{%- set primary_b2b_logic = "CASE WHEN t.deployment_group in ('B2B / Gov Paid Contract','BetterUp') AND ca.role = 'primary' and be.is_professional_coaching_cloud = 1" %}

WITH appointments AS (

    SELECT * FROM {{ ref('stg_app__appointments') }}
),

billable_event_session_details as (

    select
        associated_record_id,
        max(case
             when coaching_cloud = 'professional'
             then 1 
             else 0 end) as is_professional_coaching_cloud
    from {{ ref('stg_app__billable_events') }}
    group by 1
),

coach_assignments AS (

    SELECT * FROM {{ ref('stg_app__coach_assignments') }}

),

track_assignments AS (

    SELECT * FROM {{ ref('stg_app__track_assignments') }}
),

product_subscription_assignments AS (

    SELECT * FROM {{ ref('int_app__product_subscription_assignments') }}
),

tracks AS (

    SELECT * FROM {{ ref('dim_tracks') }}
),

/*
window calculations for 
    date of first primary b2b appointment in coach assignment
    date of most recent primary b2b appointment in coach assignment
    index of primary b2b appointment in coach assignment
    days since first primary b2b appointment in coach assignment
*/

sessions_and_assignments AS (

  SELECT
    ca.coach_id,
    ca.coach_assignment_id,
    app.member_id,
    app.appointment_id,
    app.complete_at AS session_date,
    t.deployment_group,
    t.deployment_type,
    t.minutes_limit,
    ca.role AS coaching_assignment_role, 
    be.is_professional_coaching_cloud,
    iff({{ primary_b2b_logic }} then true end IS NULL, NULL, 1) AS is_primary_b2b,
    
    MAX({{primary_b2b_logic}} THEN coalesce(ta.ended_at,psa.ended_at,current_date) END) 
        OVER (PARTITION BY ca.coach_assignment_id)
        AS primary_b2b_member_access_ended_date, 

    MIN({{primary_b2b_logic}} THEN app.complete_at END) 
        OVER (PARTITION BY ca.coach_assignment_id)
        AS first_completed_primary_b2b_coach_assignment_session_date,
  
    MAX({{primary_b2b_logic}} THEN app.complete_at END)
        OVER (PARTITION BY ca.coach_assignment_id)
        AS most_recent_completed_primary_b2b_coach_assignment_session_date
         
  FROM appointments AS app 
  LEFT JOIN billable_event_session_details as be on be.associated_record_id = app.appointment_id
  LEFT JOIN coach_assignments AS ca ON ca.coach_assignment_id = app.coach_assignment_id
  LEFT JOIN track_assignments AS ta ON ta.track_assignment_id = app.track_assignment_id 
  LEFT JOIN product_subscription_assignments AS psa ON psa.product_subscription_assignment_id = app.product_subscription_assignment_id
  LEFT JOIN tracks AS t ON t.track_id = ta.track_id 
  WHERE app.is_completed -- ignore non-completed sessions
  )

select 
    coach_id, 
    member_id,
    session_date as event_at,
    'completed' as event_action,
    'coaching session' as event_object,
    event_action || ' ' || event_object as event_action_and_object,
    'Appointment' as associated_record_type,
    appointment_id as associated_record_id,
    object_construct('first_completed_primary_b2b_coach_assignment_session_date',first_completed_primary_b2b_coach_assignment_session_date,
                      'most_recent_completed_primary_b2b_coach_assignment_session_date',most_recent_completed_primary_b2b_coach_assignment_session_date,
                      'days_since_first_completed_primary_b2b_coach_assignment_session',datediff(DAY,first_completed_primary_b2b_coach_assignment_session_date,session_date),
                      'deployment_group',deployment_group,
                      'deployment_type', deployment_type,
                      'coach_assignment_id',coach_assignment_id,
                      'coaching_assignment_role',coaching_assignment_role,
                      'primary_b2b_member_access_ended_date',primary_b2b_member_access_ended_date,
                      'is_professional_coaching_cloud',is_professional_coaching_cloud,
                      'minutes_limit',minutes_limit
                      --we use these attributes to filter and define measures in downstream models and looker
                     ) as attributes
from sessions_and_assignments
