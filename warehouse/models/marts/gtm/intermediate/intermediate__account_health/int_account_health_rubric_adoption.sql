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

)

select 
    accounts.sfdc_account_id,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087338/Adoption+Health+Score+Calculation#How-are-the-%25s-calculated%3F
    count(distinct case
                    when member_platform_calendar.first_invited_to_track_at < dateadd('month', -1, current_timestamp())
                         and member_platform_calendar.first_invited_to_track_at >= dateadd('month', -13, current_timestamp()) 
                    then member_platform_calendar.member_id else null end) as invited_members_past_30_days,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087338/Adoption+Health+Score+Calculation#How-are-the-%25s-calculated%3F   
    count(distinct case
                    when member_platform_calendar.first_invited_to_track_at < dateadd('month', -1, current_timestamp()) 
                         and member_platform_calendar.first_invited_to_track_at >= dateadd('month', -13, current_timestamp())
                         and member_platform_calendar.activated_at is not null
                    then member_platform_calendar.member_id else null end) as activated_members_past_30_days_from_invited,

    --documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087338/Adoption+Health+Score+Calculation#How-are-the-%25s-calculated%3F
    count(distinct case
                    when members.activated_at < dateadd('month', -1, current_timestamp())
                         and members.activated_at >= dateadd('month', -13, current_timestamp()) 
                    then members.member_id else null end) as members_past_30_days,
    
    ----documentation on this calculation here https://betterup.atlassian.net/wiki/spaces/AN/pages/3148087338/Adoption+Health+Score+Calculation#How-are-the-%25s-calculated%3F
    count(distinct case
                    when members.activated_at is not null
                         and member_engagement_events.eventable_type in ('Appointment','GroupCoachingCohort')
                         and member_engagement_events.verb in ('completed','joined')
                         and members.activated_at < dateadd('month', -1, current_timestamp())
                         and members.activated_at >= dateadd('month', -13, current_timestamp())
                    then members.member_id else null end) as members_with_1_session_past_30_days,
    
    (activated_members_past_30_days_from_invited * 1.00) / nullif(invited_members_past_30_days,0) as percent_invited_members_activated_past_30_days,
    (members_with_1_session_past_30_days * 1.00) / nullif(members_past_30_days,0) as percent_members_with_1_session_past_30_days,

    --NOTE - make sure to update the divisor of average_of_metrics when additional metrics are added 
    (percent_invited_members_activated_past_30_days + percent_members_with_1_session_past_30_days) / 2 as average_of_metrics,

    --categorical scores
    case
        when percent_invited_members_activated_past_30_days > 0.85
            then 'Good'
        when percent_invited_members_activated_past_30_days between 0.7 and 0.85
            then 'Okay'
        when percent_invited_members_activated_past_30_days < 0.7
            then 'Poor' else null end as account_health_rubric_adoption_driver_percent_activated,
    case
        when percent_members_with_1_session_past_30_days > 0.8
            then 'Good'
        when percent_members_with_1_session_past_30_days between 0.6 and 0.8
            then 'Okay'
        when percent_members_with_1_session_past_30_days < 0.6
            then 'Poor' else null end as account_health_rubric_adoption_driver_percent_completed_first_session,
    case
        when (percent_invited_members_activated_past_30_days is null or percent_members_with_1_session_past_30_days is null)
            then 'Not Enough Data'
        when average_of_metrics > 0.8
            then 'Good'
        when average_of_metrics between 0.6 and 0.8
            then 'Okay'
        when average_of_metrics < 0.6
            then 'Poor' end as account_health_rubric_adoption_overall_score
    

from 
member_platform_calendar
left join member_engagement_events
    on member_platform_calendar.date_key = member_engagement_events.date_key
    and member_platform_calendar.member_id = member_engagement_events.user_id
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

group by accounts.sfdc_account_id
having invited_members_past_30_days > 0
