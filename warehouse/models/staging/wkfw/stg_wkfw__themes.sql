WITH themes AS (

  SELECT * FROM {{ source('wkfw', 'themes') }}

)


SELECT
  id AS theme_id,
  theme_key,
  {{ load_timestamp('start_date') }},
  {{ load_timestamp('end_date') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM themes
