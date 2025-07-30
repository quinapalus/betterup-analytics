{{
  config(
    tags=['eu']
  )
}}

WITH construct_reference_populations AS (

  SELECT * FROM {{ source('assessment', 'construct_reference_populations') }}

),

destroyed_records AS (

  SELECT * FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'ConstructReferencePopulation'

)

SELECT
  id AS construct_reference_population_id,
  uuid AS construct_reference_population_uuid,
  construct_id,
  reference_population_id,
  reference_population_subgroup_id,
  score_mean,
  score_standard_deviation,
  created_at,
  updated_at
FROM construct_reference_populations as crp
LEFT JOIN destroyed_records AS v ON crp.id = v.item_id
WHERE v.item_id IS NULL
