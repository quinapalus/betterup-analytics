
with member_platform_calendar as (
    select * from {{ ref('member_platform_calendar')}}
),
member_engagement_events as (
    select * from {{ ref('member_engagement_events')}}
),
tracks as (
    select * from {{ ref('dim_tracks')}}
),
organizations as (
    select * from {{ ref('dim_organizations')}}
),
final as (
    select 1 as primary_key
    , count(distinct member_engagement_events.engagement_event_id ) as count_events
    from member_platform_calendar
    left join member_engagement_events 
        on member_platform_calendar.date_key = member_engagement_events.date_key 
        and member_platform_calendar.member_id = member_engagement_events.user_id
    left join tracks 
        on member_platform_calendar.track_id = tracks.track_id
    left join organizations 
        on tracks.organization_id = organizations.organization_id
    where member_engagement_events.eventable_type in ('Appointment', 'GroupCoachingCohort') 
    and member_engagement_events.verb in ('completed', 'joined') 
    and (tracks.deployment_group <> 'Test' or tracks.deployment_group is null) and (( member_platform_calendar.v2 and organizations.has_migrated_to_maps ) or
                        ( not member_platform_calendar.v2 and (not organizations.has_migrated_to_maps or organizations.has_migrated_to_maps is null) ) or
                        ( member_platform_calendar.date <= (to_char(date_trunc('second', convert_timezone('UTC', 'America/Los_Angeles', cast(organizations.v2_psa_enabled_at  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AND NOT member_platform_calendar.v2 ) OR
                        ( member_platform_calendar.v2 is null ) )
)
select * from final