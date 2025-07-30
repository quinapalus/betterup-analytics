{{
  config(
    tags=['eu']
  )
}}

WITH construct_assessments AS (

  SELECT * FROM {{ source('assessment', 'construct_assessments') }}

),

destroyed_records AS (

  SELECT * FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'ConstructAssessment'

)

SELECT 
  id AS construct_assessment_id,
  construct_id,
  assessment_type,
  created_at,
  updated_at
FROM construct_assessments as ca
LEFT JOIN destroyed_records AS v ON ca.id = v.item_id
WHERE v.item_id IS NULL

