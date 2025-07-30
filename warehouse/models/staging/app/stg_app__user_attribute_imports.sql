{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH user_attribute_imports AS (

  SELECT * FROM {{ source('app', 'user_attribute_imports') }}

)


SELECT
  id AS user_attribute_import_id,
  organization_id,
  user_id,
  {{ load_timestamp('processed_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM user_attribute_imports
