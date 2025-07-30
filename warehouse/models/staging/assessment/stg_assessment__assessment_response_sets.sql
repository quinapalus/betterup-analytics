{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH assessment_response_sets AS (

  SELECT * FROM {{ source('assessment', 'assessment_response_sets') }}

)


SELECT
  id AS assessment_response_set_id,
  key,
  item_type,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessment_response_sets