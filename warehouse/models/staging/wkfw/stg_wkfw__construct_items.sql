WITH construct_items AS (

  SELECT * FROM {{ source('wkfw', 'construct_items') }}

)


SELECT
  id AS construct_item_id,
  construct_id,
  is_scale_inverted,
  question_key,
  scale_length,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM construct_items
