{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH assessment_response_options AS (

  SELECT * FROM {{ source('assessment', 'assessment_response_options') }}

)


SELECT
  id AS assessment_response_option_id,
  assessment_response_set_id,
  position,
  translation_key,
  value,
  {{ sanitize_i18n_field('label') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_response_options