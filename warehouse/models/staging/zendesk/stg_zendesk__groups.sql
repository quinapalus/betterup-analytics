{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH groups AS (

  SELECT * FROM {{ source('zendesk', 'groups') }}

)


SELECT
  id::BIGINT AS group_id,
  name,
  created_at,
  updated_at
FROM groups 
