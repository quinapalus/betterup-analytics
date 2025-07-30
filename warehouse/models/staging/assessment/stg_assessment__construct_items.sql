{{
  config(
    tags=['eu']
  )
}}

WITH construct_items AS (

  SELECT * FROM {{ source('assessment', 'construct_items') }}

),

destroyed_records AS (

  SELECT * FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'ConstructItem'

)

SELECT 
  id AS construct_item_id,
  construct_id,
  key,
  is_scale_inverted,
  scale_length,
  reference_population_subgroup_id,
  created_at,
  updated_at
FROM construct_items as ci
LEFT JOIN destroyed_records AS v ON ci.id = v.item_id
WHERE v.item_id IS NULL
