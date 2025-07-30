{{
  config(
    tags=['eu']
  )
}}

WITH reference_populations AS (

  SELECT * FROM {{ source('assessment', 'reference_populations') }}

)

SELECT
  id AS reference_population_id,
  key,
  description,
  created_at,
  updated_at
FROM reference_populations
