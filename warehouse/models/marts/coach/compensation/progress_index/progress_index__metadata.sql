{{ config(
    tags=["eu"],
    schema="coach"
) }}

with assessments as (
    select * from {{ ref('stg_app__assessments') }}
),
appointments as (
    select * from {{ ref('stg_app__appointments') }}
),
track_assignments as (
    select * from {{ ref('stg_app__track_assignments') }}
),
tracks as (
    select * from {{ ref('dim_tracks') }}
),
coach_assignments as (
    select * from {{ ref('stg_app__coach_assignments') }}
),
users as (
    select * from {{ ref('int_app__users') }}
),
specialist_verticals as (
    select * from {{ ref('stg_curriculum__specialist_verticals') }}
),
product_subscription_assignments as (
    select * from {{ ref('stg_app__product_subscription_assignments') }}
),
product_subscriptions as (
    select * from {{ ref('stg_app__product_subscriptions') }}
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

assessment_scores as (
    select
        assessment_id
        , created_at
        , submitted_at
        , user_id
        , response.path as assessment_item
        , response.value::int as assessment_score
    from  assessments
    inner join lateral flatten (input => assessments.responses) as response
    where assessments.type = 'Assessments::PostSessionMemberAssessment'
        and response.path in ('coach_impact_development', 'coach_goal_progress', 'coach_self_awareness', 'coach_equiped_changes', 'coach_overcome_obstacles')
),

metadata as (
    select
        {{ dbt_utils.surrogate_key(['coaches.user_uuid', 'members.user_uuid', 'a.appointment_id', 's.assessment_item']) }} as unique_key
        , {{ dbt_utils.surrogate_key(['coaches.user_uuid', 'members.user_uuid']) }} as coach_member_key
        , a.member_id as member_id
        , members.user_uuid as member_uuid
        , a.coach_id as coach_id
        , coaches.user_uuid as coach_uuid
        , coach_assignments.created_at as coach_assignment_created_at
        , a.appointment_id
        , a.appointment_created_at
        , a.started_at as appointment_started_at
        , a.complete_at as appointment_completed_at
        , a.canceled_at as appointment_canceled
        , s.assessment_id
        , s.submitted_at as assessment_submitted_at
        , s.assessment_item
        , s.assessment_score
        , tracks.deployment_type
        , tracks.deployment_group
        , tracks.track_id
        , tracks.name as track_name
        , coach_assignments.role as coach_assignment_role
        , billable_event_session_details.is_professional_coaching_cloud
        , sv.name AS en_session_type
        , case
            when ( (deployment_group in ('B2B / Gov Paid Contract', 'BetterUp', 'Marketing')
                or (deployment_type in ('trial', 'trial_care', 'trial_smb')) )
                and coach_assignment_role = 'primary') then 'lead'
            when ( (deployment_group in ('B2B / Gov Paid Contract', 'BetterUp', 'Marketing')
                or (deployment_type in ('trial', 'trial_care', 'trial_smb')) )
                and coach_assignment_role = 'care') then 'care'
            when (deployment_group in ('D2C')
                or deployment_type = 'private_pay' ) then 'd2c'
            when ( (deployment_group in ('B2B / Gov Paid Contract', 'BetterUp', 'Marketing')
                or (deployment_type in ('trial', 'trial_care', 'trial_smb')) )
                and coach_assignment_role = 'secondary'
                and sv.name not in ('Sleep Coaching', 'Nutrition Coaching', 'Navigating Grief', 'Supporting Others in Grief')
                ) then 'extended network'
            when ( (deployment_group in ('B2B / Gov Paid Contract', 'BetterUp', 'Marketing')
                or (deployment_type in ('trial', 'trial_care', 'trial_smb')) )
                and coach_assignment_role = 'secondary'
                and sv.name in ('Sleep Coaching', 'Nutrition Coaching', 'Navigating Grief', 'Supporting Others in Grief')
                ) then 'extended network other'
            when ( (deployment_group in ('B2B / Gov Paid Contract', 'BetterUp', 'Marketing', 'D2C', 'private_pay')
                or (deployment_type in ('trial', 'trial_care', 'trial_smb', 'private_pay')) )
                and coach_assignment_role = 'on_demand'
                ) then 'on demand'
            else 'other' end as modality
        , object_construct('deployment_group',deployment_group
                            , 'deployment_type', deployment_type
                            , 'coach_assignment_role', coach_assignment_role
                            , 'extended_network', sv.name) as modality_attributes
    from appointments as a
    left join users as members
        on members.user_id = a.member_id
    left join users as coaches
        on coaches.user_id = a.coach_id
    left join coach_assignments
        on coach_assignments.coach_assignment_id = a.coach_assignment_id
    left join assessment_scores as s
        on s.assessment_id = a.post_session_member_assessment_id
    left join track_assignments
        on track_assignments.track_assignment_id = a.track_assignment_id
    left join tracks
        on tracks.track_id = track_assignments.track_id
    left join billable_event_session_details
        on billable_event_session_details.associated_record_id = a.appointment_id
    left join specialist_verticals AS sv
        on coach_assignments.specialist_vertical_id = sv.specialist_vertical_id
    --filter to only include completed sessions
    where appointment_completed_at is not null
)


select * from metadata
