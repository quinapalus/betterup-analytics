with session_credits as (
    select * from {{ ref('stg_app__session_credits')}}
),

appointments as (
    --not all flex sessions currently exist in appointments if not scheduled.
    select * from {{ ref('stg_app__appointments')}}
),

flex_sessions as (
    select
        session_credits.*
    from session_credits
    where is_flex_session
),

joined as (
    select
        --foreign keys
        --to be joined in downstream looker explores
        flex_sessions.session_credit_id,
        flex_sessions.appointment_id,
        flex_sessions.created_by_user_id,
        flex_sessions.order_id,
        flex_sessions.user_id,
        appointments.coach_id,
        appointments.coach_assignment_id,
        
        --appointment attributes to avoid having to join downstream
        appointments.appointment_created_at,
        appointments.appointment_updated_at,
        appointments.starts_at as appointment_starts_at,
        appointments.ends_at as appointment_ended_at,
        appointments.started_at as appointment_started_at,
        appointments.complete_at as appointment_completed_at,
        appointments.canceled_at as appointment_canceled_at,
        appointments.is_completed, --flag for completed sessions

        --timestamps
        flex_sessions.created_at,
        flex_sessions.voided_at
    from flex_sessions
    left join appointments
        on flex_sessions.appointment_id = appointments.appointment_id
),

final as (
    select
        --primary key
        {{ dbt_utils.surrogate_key(['session_credit_id', 'appointment_id'])}} as flex_session_id,
        *
    from joined
)

select * from final