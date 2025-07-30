{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH activities AS (

  SELECT * FROM {{ source('app', 'activities') }}

),

current_activities as (

  select
      id AS activity_id,
      user_id AS member_id,
      resource_id,
      creator_id,
      {{ load_timestamp('viewed_at') }}, -- populated for activities starting Dec 2016: https://github.com/betterup/betterup-app/issues/3768
      {{ load_timestamp('completed_at') }},
      {{ load_timestamp('favorited_at') }}, -- member saved to Bookmarked list
      rating, -- rating was re-introduced in Sept 2017
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }},
      associated_record_id,
      associated_record_type
  from activities

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_current_activities as (
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
      id AS activity_id,
      user_id AS member_id,
      resource_id,
      creator_id,
      {{ load_timestamp('viewed_at') }}, -- populated for activities starting Dec 2016: https://github.com/betterup/betterup-app/issues/3768
      {{ load_timestamp('completed_at') }},
      {{ load_timestamp('favorited_at') }}, -- member saved to Bookmarked list
      rating, -- rating was re-introduced in Sept 2017
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }},
      associated_record_id,
      associated_record_type
  from {{ ref('base_app__activities_historical') }}
)


select * from archived_current_activities
union
{% endif -%}
select * from current_activities

