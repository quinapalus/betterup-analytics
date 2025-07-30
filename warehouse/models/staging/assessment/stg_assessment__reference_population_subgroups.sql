{{
  config(
    tags=['eu']
  )
}}

WITH reference_population_subgroups AS (

  SELECT * FROM {{ source('assessment', 'reference_population_subgroups') }}

)

SELECT
  id AS reference_population_subgroup_id,
  key,
  description,
  created_at,
  updated_at
FROM reference_population_subgroups
