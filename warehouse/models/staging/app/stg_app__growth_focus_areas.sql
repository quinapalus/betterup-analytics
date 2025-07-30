WITH growth_focus_areas AS (

  SELECT * FROM {{ source('app', 'growth_focus_areas') }}

)


SELECT
  id AS growth_focus_area_id,
  PARSE_JSON(name) AS name,
  position,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM growth_focus_areas
