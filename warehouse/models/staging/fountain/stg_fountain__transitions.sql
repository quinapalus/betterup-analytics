{{
  config(
    tags=['eu']
  )
}}

WITH transitions AS (

  SELECT * FROM {{ source('fountain', 'transitions') }}

)

SELECT
  {{ dbt_utils.surrogate_key(['applicant_id', 'stage_id','created_at']) }}  as primary_key,
  applicant_id AS fountain_applicant_id,
  stage_id,
  stage_name,
  created_at
FROM transitions
