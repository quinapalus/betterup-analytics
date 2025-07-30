{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH assessment_items AS (

  SELECT * FROM {{ source('assessment', 'assessment_items') }}

)


SELECT
  id AS assessment_item_id,
  uuid as assessment_item_uuid,
  assessment_response_set_id,
  item_type,
  key,
  translation_key,
  PARSE_JSON(prompt_i18n):en::VARCHAR AS prompt,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }},
  PROMPT_I18N
FROM assessment_items