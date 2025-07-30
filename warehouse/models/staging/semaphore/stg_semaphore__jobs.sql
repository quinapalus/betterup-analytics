{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH jobs AS (

  SELECT * FROM {{ source('semaphore', 'job') }}

)


SELECT
  id AS job_id,
  name AS job_name,
  project_id,
  workflow_id,
  git_repo,
  branch,
  git_sha,
  result,
  create_time AS created_at,
  update_time AS updated_at,
  start_time AS started_at,
  finish_time AS finished_at
FROM jobs
ORDER BY create_time DESC
