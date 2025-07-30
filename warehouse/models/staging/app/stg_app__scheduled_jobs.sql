{{
  config(
    tags=['eu']
  )
}}

WITH scheduled_jobs AS (

  SELECT * FROM {{ source('app', 'scheduled_jobs') }}

)


SELECT
  id AS scheduled_job_id,
  job_type,
  {{ load_timestamp('perform_at') }},
  scheduling_user_id,
  sidekiq_job_id,
  {{ load_timestamp('enqueued_at') }},
  {{ load_timestamp('started_at') }},
  {{ load_timestamp('completed_at') }},
  status,
  message,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM scheduled_jobs
