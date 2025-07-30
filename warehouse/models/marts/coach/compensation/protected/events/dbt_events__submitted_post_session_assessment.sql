WITH billable_event_session_details as (

    select
        associated_record_id,
        max(case
             when coaching_cloud = 'professional'
             then 1 
             else 0 end) as is_professional_coaching_cloud
    from {{ ref('stg_app__billable_events') }}
    group by 1
),

appointments AS (

    SELECT * FROM {{ ref('stg_app__appointments') }}
),

coach_assignments AS (

    SELECT * FROM {{ ref('stg_app__coach_assignments') }}

),

track_assignments AS (

    SELECT * FROM {{ ref('stg_app__track_assignments') }}
),


tracks AS (

    SELECT * FROM {{ ref('dim_tracks') }}
),

assessments AS (

    SELECT * FROM {{ ref('stg_app__assessments') }}

),

completed_assessments AS (

    SELECT 
        appointments.coach_id,
        appointments.member_id,
        assessments.assessment_id,
        assessments.submitted_at,
        coach_assignments.specialist_vertical_uuid,
        coach_assignments.specialist_vertical_id,
        coach_assignments.role AS coaching_assignment_role,
        tracks.deployment_group,
        tracks.deployment_type,
        is_professional_coaching_cloud
        

    FROM appointments
    LEFT JOIN assessments 
        ON appointments.post_session_member_assessment_id = assessments.assessment_id
    left join billable_event_session_details 
        on billable_event_session_details.associated_record_id = appointments.appointment_id
    LEFT JOIN coach_assignments 
        ON coach_assignments.coach_assignment_id = appointments.coach_assignment_id
    LEFT JOIN track_assignments
        ON track_assignments.track_assignment_id = appointments.track_assignment_id
    LEFT JOIN tracks
        ON tracks.track_id = track_assignments.track_id
    WHERE 
        appointments.is_completed --ignore non completed sessions
        AND appointments.post_session_member_assessment_id IS NOT NULL
        
),

post_session_assessment_scores as (

  select
    assessments.assessment_id,
    response.value as assessment_response,
    case 
      when assessment_response = 'Life Changing' then 5
      when assessment_response = 'Amazing' then 4
      when assessment_response = 'Good' then 3
      when assessment_response = 'Okay' then 2
      when assessment_response = 'Not Great' then 1
    end as assessment_response_score
  from assessments
  inner join lateral flatten 
    (input => assessments.responses) as response
  where assessments.type = 'Assessments::PostSessionMemberAssessment'
  and response.path  = 'session_overall_emotional')

select
    coach_id, 
    member_id,
    submitted_at AS event_at,
    'submitted' AS event_action,
    'post session assessment' AS event_object,
    event_action || ' ' || event_object AS event_action_and_object,
    'Assessment' AS associated_record_type,
    completed_assessments.assessment_id AS associated_record_id,
    object_construct('coaching_assignment_role',coaching_assignment_role,
                      'deployment_group',deployment_group,
                      'deployment_type',deployment_type,
                      'assessment_response_score',assessment_response_score,
                      'assessment_response',assessment_response,
                      'is_professional_coaching_cloud',is_professional_coaching_cloud,
                      'specialist_vertical_uuid',specialist_vertical_uuid,
                      'specialist_vertical_id',specialist_vertical_id) AS attributes
                      --we use these attributes to filter and create measures in downstream models and looker
from completed_assessments
left join post_session_assessment_scores
    on post_session_assessment_scores.assessment_id = completed_assessments.assessment_id