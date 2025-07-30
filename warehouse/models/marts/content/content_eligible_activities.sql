WITH

eligible_resources AS (
  SELECT * FROM {{ ref('content_eligible_resources') }}
  ),

activities AS (
  SELECT *
  FROM {{ ref('stg_app__activities') }}
  WHERE
    created_at < current_date - interval '1 month'
  )

SELECT DISTINCT
  member_id,
  resource_id,
  popularity_rank,
  max(CASE WHEN viewed_at IS NOT NULL THEN 1 ELSE 0 END) AS viewed,
  max(CASE WHEN completed_at IS NOT NULL THEN 1 ELSE 0 END) AS completed
FROM activities
INNER JOIN eligible_resources
  USING(resource_id)
GROUP BY
  member_id,
  resource_id,
  popularity_rank
