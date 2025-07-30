{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH pull_requests AS (

  SELECT * FROM {{ source('github', 'pull_requests') }}

)


SELECT
  id AS pull_request_id,
  number AS pull_request_number,
  SPLIT_PART(BASE:"label",':',1)||'/'||REPLACE(BASE:"repo":"name",'"','') AS repository,
  title,
  body,
  PARSE_JSON(user):login as username,
  state,
  labels,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('closed_at') }},
  {{ load_timestamp('merged_at') }},
  {{ load_timestamp('updated_at') }}
FROM pull_requests
ORDER BY created_at DESC
