{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH tickets AS (

  SELECT * FROM {{ source('zendesk', 'tickets') }}

)


SELECT
  --ids
  id::BIGINT AS ticket_id,
  assignee_id,
  group_id,
  requester_id,
  submitter_id,
  satisfaction_rating_id,

  --fields
  status,
  priority,
  reason,
  tags,
  satisfaction_rating_score,
  satisfaction_rating_comment,

  --dates
  created_at,
  received_at,
  updated_at
FROM
  tickets
