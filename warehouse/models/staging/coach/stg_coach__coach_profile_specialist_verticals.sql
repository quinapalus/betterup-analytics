WITH coach_profile_specialist_verticals AS (

  select * from {{ source('app', 'coach_profile_specialist_verticals') }}

),

current_coach_profile_specialist_verticals as (

  select
        id as coach_profile_specialist_vertical_id,
        uuid as coach_profile_specialist_vertical_uuid,
        coach_profile_id,  -- still functional but only for US data
        coach_profile_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        specialist_vertical_id, -- still functional but only for US data
        specialist_vertical_uuid
  from coach_profile_specialist_verticals

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_coach_profile_specialist_verticals as (
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
        id as coach_profile_specialist_vertical_id,
        uuid as coach_profile_specialist_vertical_uuid,
        coach_profile_id,  -- still functional but only for US data
        coach_profile_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        specialist_vertical_id, -- still functional but only for US data
        specialist_vertical_uuid
  from {{ ref('base_app__coach_profile_specialist_verticals_historical') }}
)


select * from archived_coach_profile_specialist_verticals
union
{% endif -%}
select * from current_coach_profile_specialist_verticals


