{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH goal_check_in_emotions AS (

  SELECT * FROM {{ source('app', 'goal_check_in_emotions') }}

)


SELECT
  id AS goal_check_in_emotion_id,
  {{ sanitize_i18n_field('description') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM goal_check_in_emotions
