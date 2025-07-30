{{
  config(
    tags=["eu"]
  )
}}

WITH growth_focus_area_selections AS (

  SELECT * FROM {{ ref('stg_app__growth_focus_area_selections') }}

),

growth_focus_areas AS (

  SELECT * FROM {{ ref('stg_app__growth_focus_areas') }}

),

growth_maps AS (

  SELECT * FROM {{ ref('stg_app__growth_maps') }}

)


SELECT gfas.*, gfa.name, gfa.position, gm.member_id
FROM growth_focus_area_selections AS gfas
INNER JOIN growth_focus_areas AS gfa ON gfas.growth_focus_area_id = gfa.growth_focus_area_id
INNER JOIN growth_maps AS gm ON gfas.growth_map_id = gm.growth_map_id
