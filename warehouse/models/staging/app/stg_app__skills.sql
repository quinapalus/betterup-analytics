{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH skills AS (

  SELECT * FROM {{ source('app', 'skills') }}

)


SELECT
  id AS skill_id,
  {{ sanitize_i18n_field('name') }}
  {{ sanitize_i18n_field('overview') }}
  subdimension_key AS subdimension,
  wpm_versions, 
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM skills
