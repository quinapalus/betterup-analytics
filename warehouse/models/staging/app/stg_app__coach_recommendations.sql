WITH coach_recommendations AS (

  select * from {{ source('app', 'coach_recommendations') }}

),

current_coach_recommendations as (

  select
    id AS coach_recommendation_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }},
    user_id AS member_id,
    algorithm,
    available_during_work_week,
    coach_id,
    coach_recommendation_set_id,
    overall_score,
    position,
    upcoming_available_hours
  from coach_recommendations

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_coach_recommendations as (
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
    id AS coach_recommendation_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }},
    user_id AS member_id,
    algorithm,
    available_during_work_week,
    coach_id,
    coach_recommendation_set_id,
    overall_score,
    position,
    upcoming_available_hours
  from {{ ref('base_app__coach_recommendations_historical') }}
)


select * from archived_coach_recommendations
union
{% endif -%}
select * from current_coach_recommendations

