{{
  config(
    tags=['eu']
  )
}}

WITH constructs AS (

  SELECT * FROM {{ source('assessment', 'constructs') }}

),

destroyed_records AS (

  SELECT * FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'Construct'

)

SELECT
  id AS construct_id,
  uuid as construct_uuid,
  -- key and name are reserved words in some versions of the SQL specification
  key as construct_key,
  name as construct_name,
  label_i18n,
  scale_mean,
  scale_standard_deviation,
  scale_min_score,
  scale_max_score,
  created_at,
  updated_at
FROM constructs as c
LEFT JOIN destroyed_records AS v ON c.id = v.item_id
WHERE v.item_id IS NULL

