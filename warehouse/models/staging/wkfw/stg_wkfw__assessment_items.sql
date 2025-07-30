WITH assessment_items AS (

  SELECT * FROM {{ source('wkfw', 'assessment_items') }}

)


SELECT
  id AS assessment_item_id,
  is_reverse,
  is_skippable,
  position,
  {{ sanitize_i18n_field('subheading') }}
  {{ sanitize_i18n_field('prompt') }}
  question_key,
  question_type,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_items
