with daily_status as (
    select * from {{ ref('int_coach_member_retention_daily_status')}}
),

final as (
    select
        ---primary key
        _unique,

        --attributes
        date_day,
        coach_id,
        member_id,

        --attributes
        coach_member_first_session_completed_at,
        coach_member_assignment_ended_at,
        is_active_coach_member_assignment,

        --cohort values
        days_from_first_coach_member_completed_session,
        weeks_from_first_coach_member_completed_session,
        months_from_first_coach_member_completed_session
    from daily_status
)

select * from final 