{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH goal_obstacles AS (

  SELECT * FROM {{ source('app', 'goal_obstacles') }}

)


SELECT
  id AS goal_obstacle_id,
  {{ sanitize_i18n_field('description') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM goal_obstacles
