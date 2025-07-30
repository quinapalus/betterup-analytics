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

members as (

    select * from {{ ref('dim_members') }}

),

member_platform_calendar as (

    select * from {{ ref('member_platform_calendar') }}

)

    /*
    returns only accounts that meet criteria documented here 
    --https://betterup.atlassian.net/wiki/spaces/AN/pages/3141402630/Member+Populations+and+Timing+of+Metrics+in+Account+Health+2.0+Model#Excluded-Accounts-Cheatsheet
    */

select
    distinct
    accounts.sfdc_account_id,
    true as is_eligible_for_account_health_scoring
from 
member_platform_calendar
left join tracks
    on tracks.track_id = member_platform_calendar.track_id
inner join organizations
    on organizations.organization_id = tracks.organization_id
left join accounts
    on accounts.sfdc_account_id = organizations.sfdc_account_id
inner join members
    on members.member_id = member_platform_calendar.member_id

where

    member_platform_calendar.primary_coaching
    and tracks.deployment_group = 'B2B / Gov Paid Contract'
    and accounts.sfdc_account_id is not null
    and 
        (members.activated_at >= dateadd('month', -15, current_timestamp)
        and members.activated_at < dateadd('month', 16, dateadd('month', -15, current_timestamp))) 
    --the below logic comes from always_filter conditions in the Member Engagement Events on Platform explore
    and (
        (member_platform_calendar.v2
        and organizations.v2_psa_enabled_at is not null)

        or 
        (not member_platform_calendar.v2 and organizations.v2_psa_enabled_at is null)
         or (member_platform_calendar.date <= organizations.v2_psa_enabled_at and not member_platform_calendar.v2)
         or (member_platform_calendar.v2 is null))

