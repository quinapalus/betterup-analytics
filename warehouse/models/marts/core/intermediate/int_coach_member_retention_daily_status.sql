with util_day as (
    select * from {{ ref('util_day')}}
),

util_month as (
    select * from {{ ref('util_month')}}
),

coach_member_first_sessions as (
    select * from {{ ref('stg_app__appointments')}}
    where member_coach_session_completed_order = 1
    --ensure completed at is not null
),

coach_assignments as (
    select * from {{ ref('stg_app__coach_assignments')}}
),

coach_member_join as (
    select 
        coach_member_first_sessions.coach_id,
        coach_member_first_sessions.member_id,
        coach_member_first_sessions.coach_assignment_id,
        coach_member_first_sessions.complete_at as coach_member_first_session_completed_at,
        coach_assignments.ended_at as coach_member_assignment_ended_at
    from coach_member_first_sessions
    left join coach_assignments
        on coach_member_first_sessions.coach_assignment_id = coach_assignments.coach_assignment_id
),

joined as (
    select
        util_day.date_day,
        coach_member_join.coach_id,
        coach_member_join.member_id,
        coach_member_join.coach_member_first_session_completed_at,
        coach_member_join.coach_member_assignment_ended_at
    from util_day
    cross join coach_member_join
    where true
        -- calendar date greater than or equal to start date to fan out records for all future dates
        and util_day.date_day >= date_trunc('day', coach_member_join.coach_member_first_session_completed_at)
        and util_day.date_day <= coach_member_join.coach_member_assignment_ended_at
),

final as (
    select
        ---primary key
        {{ dbt_utils.surrogate_key(['date_day', 'coach_id', 'member_id'])}} as _unique,
        date_day,

        --foreign keys
        coach_id,
        member_id,
        
        --attributes
        coach_member_first_session_completed_at,
        coach_member_assignment_ended_at,

        --booleans
        iff(date_day != coach_member_assignment_ended_at::date, true, false) as is_active_coach_member_assignment,
       
        --cohort values
        datediff('day', coach_member_first_session_completed_at, date_day) as days_from_first_coach_member_completed_session,
        datediff('week', coach_member_first_session_completed_at, date_day) as weeks_from_first_coach_member_completed_session,
        datediff('month', coach_member_first_session_completed_at, date_day) as months_from_first_coach_member_completed_session

    from joined
)

select * from final
