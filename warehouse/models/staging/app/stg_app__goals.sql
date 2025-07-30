{{
  config(
    tags=['classification.c2_restricted']
  )
}}


WITH goals AS (

  select * from {{ source('app', 'goals') }}

),

current_goals as (

  select
      id AS goal_id,
      user_id AS member_id,
      description,
      topic_theme_id,
      objective_outcome_id AS goal_outcome_id,
      objective_outcome_custom_text AS goal_outcome_custom_text,
      objective_obstacle_id AS goal_obstacle_id,
      objective_obstacle_custom_text AS goal_obstacle_custom_text,
      objective_action_id AS goal_action_id,
      objective_action_custom_text AS goal_action_custom_text,
      development_topic_id,
      {{ load_timestamp('completed_at') }},
      creator_id,
      {{ load_timestamp('ends_at') }},
      shared_with_manager,
      last_user_progress_response AS last_member_progress_response_percent,
      {{ load_timestamp('archived_at') }},
      last_manager_progress_response AS last_manager_progress_response_percent,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from goals

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_goals as (
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
      id AS goal_id,
      user_id AS member_id,
      description,
      topic_theme_id,
      objective_outcome_id AS goal_outcome_id,
      objective_outcome_custom_text AS goal_outcome_custom_text,
      objective_obstacle_id AS goal_obstacle_id,
      objective_obstacle_custom_text AS goal_obstacle_custom_text,
      objective_action_id AS goal_action_id,
      objective_action_custom_text AS goal_action_custom_text,
      development_topic_id,
      {{ load_timestamp('completed_at') }},
      creator_id,
      {{ load_timestamp('ends_at') }},
      shared_with_manager,
      last_user_progress_response AS last_member_progress_response_percent,
      {{ load_timestamp('archived_at') }},
      last_manager_progress_response AS last_manager_progress_response_percent,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__objectives_historical') }}
)


select * from archived_goals
union
{% endif -%}
select * from current_goals





