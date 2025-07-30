WITH growth_focus_area_selections AS (

  select * from {{ source('app', 'growth_focus_area_selections') }}

),

current_growth_focus_area_selections as (

  select
      id AS growth_focus_area_selection_id,
      growth_map_id,
      growth_focus_area_id,
      satisfaction,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from growth_focus_area_selections

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_growth_focus_area_selections as (
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
      id AS growth_focus_area_selection_id,
      growth_map_id,
      growth_focus_area_id,
      satisfaction,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__growth_focus_area_selections_historical') }}
)


select * from archived_growth_focus_area_selections
union
{% endif -%}
select * from current_growth_focus_area_selections
