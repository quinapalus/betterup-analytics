with group_coaching_assignments as (
    select * from {{ref('int_group_coaching_assignments')}}
),

members as (
    select * from {{ref('dei_members')}}
),

invited as (
    select
        member_id,
        group_coaching_series_id,
        track_assignment_id,
        product_subscription_assignment_id,
        'invited' as event_type,
        greatest(ta_created_at, psa_starts_at, registration_start) as event_at,
        null as event_type_id
    from group_coaching_assignments
),

activated as (
    select
        gca.member_id,
        group_coaching_series_id,
        track_assignment_id,
        product_subscription_assignment_id,
        'activated' as event_type,
        case
            when m.confirmed_at < gca.ta_created_at then gca.ta_created_at
            when gca.ta_ended_at is null or m.confirmed_at < gca.ta_ended_at then m.confirmed_at
            else null
        end as event_at,
        null as event_type_id
    from group_coaching_assignments as gca
    inner join members as m 
        on gca.member_id = m.member_id
    where event_at is not null
),

onboarded as (
    select
        gca.member_id,
        group_coaching_series_id,
        track_assignment_id,
        product_subscription_assignment_id,
        'onboarded' as event_type,
        m.completed_member_onboarding_at as event_at,
        null as event_type_id
    from group_coaching_assignments as gca
    inner join members as m 
        on gca.member_id = m.member_id
    where m.completed_member_onboarding_at is not null
    and ((m.completed_member_onboarding_at < gca.ta_ended_at) or gca.ta_ended_at is null)
),

registered as (
    select
        gca.member_id,
        group_coaching_series_id,
        track_assignment_id,
        product_subscription_assignment_id,
        'registered' as event_type,
        created_at as event_at,
        r.group_coaching_registration_id as event_type_id
    from group_coaching_assignments as gca
    inner join {{ref('stg_app__group_coaching_registrations')}} as r 
        on gca.group_coaching_cohort_id = r.group_coaching_cohort_id
),

attended_session_n as (
    select
        gca.member_id,
        group_coaching_series_id,
        track_assignment_id,
        product_subscription_assignment_id,
        'attended session ' || s.session_number as event_type,
        s.starts_at as event_at,
        s.group_coaching_session_id as event_type_id
    from group_coaching_assignments as gca
    inner join {{ref('stg_app__group_coaching_sessions')}} as s 
        on gca.group_coaching_cohort_id = s.group_coaching_cohort_id
    inner join {{ref('int_app__group_coaching_appointments')}} as a 
        on s.group_coaching_session_id = a.group_coaching_session_id 
        and gca.member_id = a.member_id
    where a.attempted_to_join_at is not null
),

union_all as (
    select * from invited
    union all select * from activated
    union all select * from onboarded
    union all select * from registered
    union all select * from attended_session_n
),

final as (

    select
    distinct 
    {{ dbt_utils.surrogate_key(['gca.group_coaching_assignment_id','ua.event_type'
        ,'ua.event_at'])}} as group_coaching_funnel_id,
    gca.group_coaching_assignment_id,
    gca.group_coaching_series_id,
    gca.registration_start,
    gca.registration_ended_at,
    gca.track_assignment_id,
    gca.ta_created_at,
    gca.ta_ended_at,
    gca.track_id,
    gca.group_coaching_curriculum_id,
    gca.group_coaching_cohort_id,
    gca.group_coaching_registration_id,
    gca.registration_created_at,
    gca.curriculum_title,
    gca.member_id,
    gca.program_name,
    gca.product_subscription_assignment_id,
    gca.psa_starts_at,
    gca.psa_ended_at,
    gca.product_subscription_id,
    gca.product_id,
    gca.product_name,
    gca.workshops,
    gca.coaching_circles,
    ua.event_type,
    ua.event_at,
    ua.event_type_id
    from group_coaching_assignments gca
    inner join union_all ua 
        on gca.member_id = ua.member_id
        and gca.group_coaching_series_id = ua.group_coaching_series_id
        and gca.track_assignment_id = ua.track_assignment_id
        and gca.product_subscription_assignment_id = ua.product_subscription_assignment_id
)

select *
from final
