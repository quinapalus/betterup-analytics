{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH users AS (

  SELECT * FROM {{ source('zendesk', 'users') }}

)


SELECT
  --ids
  TRY_CAST(id AS BIGINT) AS user_id,
  TRY_CAST(external_id AS INT) AS app_user_id,

  --fields
  role AS zendesk_role,
  locale
FROM
  users
