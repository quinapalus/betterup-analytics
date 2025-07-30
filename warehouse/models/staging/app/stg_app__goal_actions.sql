{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH goal_actions AS (

  SELECT * FROM {{ source('app', 'goal_actions') }}

)


SELECT
  id AS goal_action_id,
  {{ sanitize_i18n_field('description') }}
  {{ sanitize_i18n_field('collaborator_description') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM goal_actions
