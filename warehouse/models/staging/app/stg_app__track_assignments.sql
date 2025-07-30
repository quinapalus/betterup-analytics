{{
  config(
    tags=["eu"]
  )
}}

WITH track_assignments AS (

  select * from {{ source('app', 'track_assignments') }}

),

current_track_assignments as (

  select
      ta.id as track_assignment_id
    , ta.created_at::timestamp_ntz as created_at
    , ta.ended_at::timestamp_ntz as ended_at
    , ta.ends_on::timestamp_ntz as ends_on
    , ta.ended_reason
    , ta.extended_network as is_extended_network_coaching_enabled
    , ta.hidden as is_hidden
    , ta.minutes_limit
    , ta.minutes_used
    , ta.on_demand as is_on_demand_coaching_enabled
    , ta.primary_coaching_enabled as is_primary_coaching_enabled
    , ta.program_change_notifications_enabled as is_program_change_notifications_enabled
    , ta.track_id
    , ta.updated_at::timestamp_ntz as updated_at
    , ta.user_id as member_id
    , ta.primary_coaching_starts_at
    , ta.care as is_care_coaching_enabled
    , ta.peer as is_peer_coaching_enabled
--    , ta.community as is_community_coaching_enabled --this column does not exist anymore in the postgres DB
  from track_assignments ta

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_track_assignments as (
/*

  The archived records in this CTE are records that have been
  deleted in source db and lost due to ingestion re-replication.

  A large scale re-replication occured in 2023-06 during the Stitch upgrade
  and the creation of the new landing schema - stitch_app_v2.
  The app_archive tables found with a tag 2023_06 hold the records
  that pertain to the deleted records at that time and reference can be found in
  ../models/staging/app/sources_schema_app.yml file.

  Details of the upgrade process & postmortem can be found in the Confluence doc titled:
  "stitch_app_v2 upgrade | Process Reference Doc"
  https://betterup.atlassian.net/wiki/spaces/DATA/pages/3418750982/stitch+app+v2+upgrade+Process+Reference+Doc

*/

  select
      id as track_assignment_id
    , created_at::timestamp_ntz as created_at
    , ended_at::timestamp_ntz as ended_at
    , ends_on::timestamp_ntz as ends_on
    , ended_reason
    , extended_network as is_extended_network_coaching_enabled
    , hidden as is_hidden
    , minutes_limit
    , minutes_used
    , on_demand as is_on_demand_coaching_enabled
    , primary_coaching_enabled as is_primary_coaching_enabled
    , program_change_notifications_enabled as is_program_change_notifications_enabled
    , track_id
    , updated_at::timestamp_ntz as updated_at
    , user_id as member_id
    , primary_coaching_starts_at
    , care as is_care_coaching_enabled
    , peer as is_peer_coaching_enabled
--    , community as is_community_coaching_enabled --this column does not exist anymore in the postgres DB
  from {{ ref('base_app__track_assignments_historical') }}
)


select * from archived_track_assignments
union
{% endif -%}
select * from current_track_assignments
