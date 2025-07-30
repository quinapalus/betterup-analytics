with tracks as (
    
    select * from {{ ref('dim_tracks') }}

),

organizations as (

    select * from {{ ref('stg_app__organizations') }}

),

accounts as (

    select * from {{ ref('int_sfdc__accounts_snapshot') }} 
    where is_current_version and not is_deleted --only want the most recent snapshots

),


eligible_accounts as (

    select * from {{ ref('int_account_health_eligible_accounts') }}
    --all accounts that meet criteria from here
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3141402630/Member+Populations+and+Timing+of+Metrics+in+Account+Health+2.0+Model#Excluded-Accounts-Cheatsheet

),

members as (

    select * from {{ ref('dim_members') }}

),

member_platform_calendar as (

    select * from {{ ref('member_platform_calendar') }}

),

member_engagement_events as (

    select * from {{ ref('member_engagement_events') }}

),

billable_events as (
    
    select * from {{ ref('stg_app__billable_events') }}

),

appointments as (
    
    select * from {{ ref('stg_app__appointments') }}

),

assessments as (
    select * from {{ ref('fact_assessments') }}
),

completed_sessions_billable_events as (
    select
        associated_record_id,
        sum(case when event_type = 'completed_sessions' then 1 else 0 end) as completed_sessions_count
    from billable_events
    where associated_record_type = 'Session'
    group by associated_record_id
),

sessions as (
    select
        appointment_id,
        member_id,
        completed_sessions_count > 0 as completed_session_billable_event
    from appointments
    left join completed_sessions_billable_events
        on completed_sessions_billable_events.associated_record_id = appointments.appointment_id
)


select 
    accounts.sfdc_account_id,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087347/Engagement+Health+Score+Calculation
    count(distinct case
                    when members.activated_at < dateadd('month', -1, current_timestamp())
                         and members.activated_at >= dateadd('month', -13, current_timestamp()) 
                    then members.member_id else null end) as members_past_30_days,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087347/Engagement+Health+Score+Calculation
    count(distinct case
                    when members.activated_at < dateadd('month', -4, current_timestamp())
                         and members.activated_at >= dateadd('month', -16, current_timestamp()) 
                    then members.member_id else null end) as members_past_120_days,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087347/Engagement+Health+Score+Calculation
    count(distinct case
                    when members.activated_at is not null
                         and member_engagement_events.eventable_type in ('Appointment','GroupCoachingCohort')
                         and member_engagement_events.verb in ('completed','joined')
                         and members.activated_at < dateadd('month', -1, current_timestamp())
                         and members.activated_at >= dateadd('month', -13, current_timestamp())
                    then members.member_id else null end) as members_with_1_session_past_30_days,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087347/Engagement+Health+Score+Calculation
    count(distinct case
                    when members.activated_at is not null
                         and member_engagement_events.eventable_type in ('Appointment','GroupCoachingCohort')
                         and member_engagement_events.verb in ('completed','joined')
                         and members.activated_at < dateadd('month', -4, current_timestamp())
                         and members.activated_at >= dateadd('month', -16, current_timestamp())
                    then members.member_id else null end) as members_with_1_session_past_120_days,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087347/Engagement+Health+Score+Calculation
    count(distinct case
                    when completed_session_billable_event
                         and members.activated_at < dateadd('month', -1, current_timestamp())
                         and members.activated_at >= dateadd('month', -13, current_timestamp())
                    then sessions.appointment_id else null end) as total_billable_sessions,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087347/Engagement+Health+Score+Calculation
    count(distinct case
                    when assessments.submitted_at is not null
                         and assessments.assessment_name like '%Reflection Point'
                         and members.activated_at < dateadd('month', -4, current_timestamp())
                         and members.activated_at >= dateadd('month', -16, current_timestamp())
                    then members.member_id else null end) as members_with_1_reflection_point_past_120_days,

    avg(case
                  when members.activated_at < dateadd('month', -1, current_timestamp())
                       and members.activated_at >= dateadd('month', -13, current_timestamp())
                  then (member_platform_calendar.total_days_in_tenure_since_activation * 1.00) / 30 else null end) as average_of_total_months_in_tenure_since_activation,
   
    (total_billable_sessions * 1.00) / nullif(members_with_1_session_past_30_days,0) / nullif(average_of_total_months_in_tenure_since_activation,0) as average_sessions_per_member_per_month,

    (members_with_1_reflection_point_past_120_days * 1.00) / nullif(members_with_1_session_past_120_days,0) as percent_of_members_with_1_reflection_point,

    case
        when average_sessions_per_member_per_month > 2.0
            then 'Good'
        when average_sessions_per_member_per_month between 1.0 and 2.0
            then 'Okay'
        when average_sessions_per_member_per_month < 1.0
            then 'Poor' else null end as account_health_rubric_adoption_driver_average_sessions_per_member_per_month,
    
    case
        when percent_of_members_with_1_reflection_point > 0.7
            then 'Good'
        when percent_of_members_with_1_reflection_point between 0.4 and 0.7
            then 'Okay'
        when percent_of_members_with_1_reflection_point < 0.4
            then 'Poor' else null end as account_health_rubric_adoption_driver_reflection_points,
    
    case
        when (average_sessions_per_member_per_month is null or percent_of_members_with_1_reflection_point is null)
            then 'Not Enough Data'
        when
            (percent_of_members_with_1_reflection_point <= 0.35 or average_sessions_per_member_per_month < 1)
            then 'Poor'
        when average_sessions_per_member_per_month >= 1.89 and percent_of_members_with_1_reflection_point >= 0.60
            then 'Good'
        when average_sessions_per_member_per_month < 1.50 and percent_of_members_with_1_reflection_point < 0.40
            then 'Poor'
        else 'Okay' end as account_health_rubric_engagement_overall_score
        
from member_platform_calendar
left join member_engagement_events
    on member_platform_calendar.date_key = member_engagement_events.date_key
    and member_platform_calendar.member_id = member_engagement_events.user_id
left join sessions
    on member_engagement_events.user_id = sessions.member_id
    and member_engagement_events.eventable_id = sessions.appointment_id
    and member_engagement_events.eventable_type = 'Appointment'
left join assessments
    on member_engagement_events.user_id = assessments.user_id
    and member_engagement_events.eventable_id = assessments.assessment_id
    and member_engagement_events.eventable_type = 'Assessment'
left join tracks
    on tracks.track_id = member_platform_calendar.track_id
inner join organizations
    on organizations.organization_id = tracks.organization_id
left join accounts
    on accounts.sfdc_account_id = organizations.sfdc_account_id
inner join eligible_accounts
    on accounts.sfdc_account_id = eligible_accounts.sfdc_account_id
inner join members
    on members.member_id = member_platform_calendar.member_id

where 
    member_platform_calendar.primary_coaching
    and tracks.deployment_group = 'B2B / Gov Paid Contract'
    and accounts.sfdc_account_id is not null

    --the below logic comes from always_filter conditions in the Member Engagement Events on Platform explore
    and (
        (member_platform_calendar.v2
        and organizations.v2_psa_enabled_at is not null)

        or 
        (not member_platform_calendar.v2 and organizations.v2_psa_enabled_at is null)
         or (member_platform_calendar.date <= organizations.v2_psa_enabled_at and not member_platform_calendar.v2)
         or (member_platform_calendar.v2 is null))
    and 
        (members.activated_at >= dateadd('month', -15, current_timestamp)
        and members.activated_at < dateadd('month', 16, dateadd('month', -15, current_timestamp))) 
    
group by accounts.sfdc_account_id
