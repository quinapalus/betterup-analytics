{{
  config(
    tags=["eu"]
  )
}}

with track_assignments as (
    select * from {{ ref('stg_app__track_assignments') }}
)
, tracks as (
    select * from {{ ref('dim_tracks') }}
)
, members as (
    select * from {{ ref('dim_members') }}
)
, organizations as (
    select * from {{ ref('dim_organizations') }}
)
, joined as (
    select
        track_assignments.*,
        members.level,
        members.job_function,
        members.geo as region,
        members.country_name,  
        tracks.name as track_name,
        tracks.program_name,
        organizations.organization_name, 
        not track_assignments.is_hidden as is_visible,
        track_assignments.ended_at is null as is_open,
        case
          -- if member is activated prior to track_assignment creation, mark track_assignment as activated on creation:
          when members.activated_at < track_assignments.created_at 
              then track_assignments.created_at
          -- if track_assignment is open, or member activated prior to track_assignment ended, use date member activated (if any):
          when track_assignments.ended_at is null or members.activated_at < track_assignments.ended_at 
              then members.activated_at
          -- in case where member activated after track_assignment ended, track_assignment.activated_at is null:
          else null
          end as activated_at,
        -- for members that have multiple track_assignments for a given track, find the first invite date:
        case
          when not track_assignments.is_hidden 
              then min(track_assignments.created_at) over (partition by track_assignments.member_id, track_assignments.track_id, track_assignments.is_hidden)
          else null
          end as member_first_invited_to_track_at   

    from track_assignments
    inner join members 
        on track_assignments.member_id = members.member_id
    left join tracks 
        on track_assignments.track_id = tracks.track_id
    left join organizations
        on tracks.organization_id = organizations.organization_id

)
, final as (
    select
        --Primary Key
        track_assignment_id,

        --Foreign Keys
        track_id,
        member_id,

        --Logical Data
        level,
        job_function,
        region,
        country_name,  
        minutes_limit,
        minutes_used,
        ended_reason,
        track_name,
        program_name,
        organization_name, 
        datediff('seconds', member_first_invited_to_track_at, activated_at) / 86400.0 as days_to_activate,

        --Booleans
        is_hidden,
        is_visible,
        is_open,
        is_extended_network_coaching_enabled,
        is_on_demand_coaching_enabled,
        is_primary_coaching_enabled,
        is_program_change_notifications_enabled,
        is_care_coaching_enabled,
        is_peer_coaching_enabled,
--        coalesce(is_community_coaching_enabled, FALSE) as is_community_coaching_enabled, --this column does not exist anymore in the postgres DB
        activated_at is not null as is_activated,

        --these will be removed in a future PR, keeping here for now to avoid breaking changes
        is_hidden as hidden, 
        is_extended_network_coaching_enabled as coaching_extended_network_enabled,
        is_on_demand_coaching_enabled as coaching_on_demand_enabled,
        is_primary_coaching_enabled as coaching_primary_enabled,

        --Timestamps
        created_at,
        ended_at,
        ends_on,
        updated_at,
        primary_coaching_starts_at,
        activated_at,
        member_first_invited_to_track_at

    from joined
   
)

select * from final
