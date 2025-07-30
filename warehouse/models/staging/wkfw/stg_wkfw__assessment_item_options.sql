WITH assessment_item_options AS (

  SELECT * FROM {{ source('wkfw', 'assessment_item_options') }}

)


SELECT
  id AS assessment_item_option_id,
  position,
  question_type,
  value,
  {{ sanitize_i18n_field('label') }}
  {{ sanitize_i18n_field('subtitle') }}
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_item_options
