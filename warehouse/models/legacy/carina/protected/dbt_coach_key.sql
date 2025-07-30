{{
  config(
    tags=['classification.c3_confidential'],
    materialized='view'
  )
}}

WITH dim_coach AS (

  SELECT * FROM {{ref('dim_coach')}}

),

coach_applicants AS (

  SELECT * FROM {{ref('dei_coach_applicants')}}

)

SELECT * FROM (

SELECT
  coach_app_coach_id AS app_coach_id,
  coach_key,
  ROW_NUMBER() OVER (
      PARTITION BY coach_app_coach_id
      ORDER BY coach_app_coach_id, ca.last_transitioned_at DESC NULLS LAST
  ) AS index
FROM dim_coach AS c
LEFT OUTER JOIN coach_applicants AS ca
  ON c.coach_fnt_applicant_id = ca.fountain_applicant_id
WHERE c.coach_app_coach_id IS NOT NULL

) a

WHERE index = 1
