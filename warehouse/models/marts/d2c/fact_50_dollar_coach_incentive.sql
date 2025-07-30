

with members as (
    select * from {{ ref('dim_members') }}
),

product_subscription_assignments as (
    select * from {{ ref('int_app__product_subscription_assignments') }}
),

sessions as (
    select * from {{ ref('fact_appointments') }}
),

coach_assignments as (
    select * from {{ ref('dim_coach_assignments') }}
),

track_assignments as (
    select * from {{ ref('dim_track_assignments') }}
),

tracks as (
    select * from {{ ref('dim_tracks') }}
),

d2c_member_subscriptions as (
    select distinct psa.member_id,
        psa.starts_at::date as subscription_start_date,
        coalesce(psa.ended_at,psa.ends_at,'12/31/2099')::date as subscription_end_date,
        (lag(coalesce(psa.ended_at,psa.ends_at)) over(partition by member_id order by psa.starts_at::date, coalesce(psa.ended_at,psa.ends_at))
           not between dateadd(day,-30,psa.starts_at::date) and coalesce(psa.ended_at,psa.ends_at,'12/31/2099'))::int as island
    from product_subscription_assignments psa
    where psa.stripe_subscription_id is not null --if there's a subscription_id then they're d2c
),

subscription_groups as (
  select *, 
         sum(coalesce(island, true)::int) over(partition by member_id order by subscription_start_date, subscription_end_date) as group_num 
    from d2c_member_subscriptions
), 

subscription_windows as ( /*creates a set of members with subscription start/end dates with subscription with a 30 day gap considered as no gap
        The 30 day gap is defined above in d2c_member_subscriptions*/
    select member_id, 
           coalesce(group_num,1) as group_num, --if the group_num is null considered it as part of the 1st group.
           min(subscription_start_date) as subscription_start_date, 
           nullif(max(subscription_end_date),'12/31/2099') as subscription_end_date
    from subscription_groups
    group by member_id, group_num
),

primary_coaching_sessions AS (
    select 
        s.member_id,
        coalesce(sw.group_num,1) as group_num,
        sw.subscription_start_date,
        sw.subscription_end_date,
        s.coach_id,
        ca.coach_assignment_id,
        ca.ended_at::date as coach_assignment_ended_date,
        s.complete_at::date as session_complete_date, 
        s.starts_at::date as session_starts_date,
        row_number() over(partition by s.member_id order by s.starts_at desc) as member_overall_sequence_number_reversed,
        row_number() over(partition by s.member_id, s.coach_id, coalesce(sw.group_num,1) order by s.starts_at) as session_sequence_number,
        row_number() over(partition by s.member_id, s.coach_id, coalesce(sw.group_num,1) order by s.starts_at desc) as session_sequence_number_reversed
    from sessions as s
    inner join coach_assignments as ca
        on s.coach_assignment_id = ca.coach_assignment_id
    inner join track_assignments ta 
        on s.track_assignment_id = ta.track_assignment_id
    inner join tracks t
        on ta.track_id = t.track_id
        and t.deployment_group = 'D2C'
    left join subscription_windows sw --purposefully left joining here as "trial" subscriptions sessions are typically outside the subscription start/end dates
        on s.member_id = sw.member_id
        and (
                (s.starts_at >= sw.subscription_start_date and s.starts_at < coalesce(sw.subscription_end_date,'12/31/2099'))
                or
                (s.starts_at < sw.subscription_start_date and sw.group_num = 1)
            )
    where ca.role = 'primary' --role = coach_type - only use primary coaching sessions
    and s.is_completed = true
),

sessions_count as
(
    select member_id, coach_id
    , group_num
    , count(*) as number_of_sessions
    from primary_coaching_sessions
    group by member_id, coach_id, group_num
),

first_primary_coaching_session_with_coach as
(
    select *,
    dateadd(day,90,session_starts_date)::date as will_reach_90_days_date
    from primary_coaching_sessions 
    where session_sequence_number = 1 --this selects the first session for a specific member and coach
),

last_primary_coaching_session_with_coach as
(
    select * 
    from primary_coaching_sessions ls
    where ls.session_sequence_number_reversed = 1 --this selects the last session for a specific member and coach
),

combined as
(
    select 
    m.member_id,
    fs.coach_id,
    --m.group_num,
    fs.session_starts_date as first_session_with_coach_date,
    ls.session_starts_date as last_session_with_coach_date,
    ls.coach_assignment_ended_date,
    case 
        when ls.member_overall_sequence_number_reversed = 1 --if this is the current coach (based on most recent session) for this member 
            and (m.subscription_end_date is null or m.subscription_end_date >= sysdate()::date) --and the subscription has not yet ended  
            then datediff(day,fs.session_starts_date,coalesce(ls.coach_assignment_ended_date,convert_timezone('UTC','America/Los_Angeles', sysdate())::timestamp_ltz)) 
        when m.subscription_start_date < fs.will_reach_90_days_date --make sure the track assignment started before the member/coach would hit the incentive 
            then datediff(day,fs.session_starts_date,case when m.subscription_end_date < ls.coach_assignment_ended_date 
                                                            then m.subscription_end_date /*If the subscription end date is less than the coach assignment end date
                                                                then use the subscription date as the end of the "window".*/
                                                          when fs.coach_id = ls.coach_id and ls.coach_assignment_ended_date is null
                                                            then m.subscription_end_date /* When comparing the first and last session with the same coach_id
                                                                and the coach_assignment_ended_date is null (kept the same coach across a gap) then use the
                                                                subscription_end_date as the end the "window".*/
                                                            else coalesce(ls.coach_assignment_ended_date, ls.session_starts_date) end)  /* else use the coach assignment end date
                                                                or last session_starts_date  as the end of the "window" - these should be a change in coach, but the subscription stays
                                                                consistent.*/
        else 0 end
        as number_of_incentive_days, 
    m.subscription_start_date,
    m.subscription_end_date,
    fs.will_reach_90_days_date,
    ac.number_of_sessions
    from subscription_windows m --d2c_member_subscriptions_distinct m
    inner join first_primary_coaching_session_with_coach fs
        on m.member_id = fs.member_id 
        and m.group_num = fs.group_num
    inner join last_primary_coaching_session_with_coach ls
        on fs.member_id = ls.member_id
        and fs.coach_id = ls.coach_id
        and m.group_num = ls.group_num
    inner join sessions_count ac 
        on fs.member_id = ac.member_id
        and fs.coach_id = ac.coach_id
        and fs.group_num = ac.group_num
),

incentive_days as 
(
    select distinct *,    
    case when number_of_incentive_days >= 90
        and exists (
            select 1 
            from subscription_windows a 
            where a.member_id = c.member_id 
            and c.will_reach_90_days_date > a.subscription_start_date 
            and c.will_reach_90_days_date <= coalesce(a.subscription_end_date,'12/31/2099')
            ) -- make sure the member was on a d2c psa at the time the incentive would have been met.
    then will_reach_90_days_date
        else null end as date_incentive_was_met_prep
    from combined c
),

final as
(
    select a.member_id,
        a.coach_id,
        a.first_session_with_coach_date,
        a.last_session_with_coach_date,
        a.coach_assignment_ended_date,
        a.number_of_incentive_days, 
        a.subscription_start_date,
        a.subscription_end_date,
        a.will_reach_90_days_date,
        a.number_of_sessions,
        case when exists (select 1 
                            from incentive_days b 
                            where a.member_id = b.member_id 
                            and a.coach_id = b.coach_id
                            and b.date_incentive_was_met_prep < a.date_incentive_was_met_prep)
            then null
            else date_incentive_was_met_prep
        end as date_incentive_was_met
    from incentive_days a
)

select *
from final
 