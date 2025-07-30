{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH goal_outcomes AS (

  SELECT * FROM {{ source('app', 'goal_outcomes') }}

)


SELECT
  id AS goal_outcome_id,
  {{ sanitize_i18n_field('description') }}
  topic_theme_id,
  {{ sanitize_i18n_field('collaborator_description') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM goal_outcomes
