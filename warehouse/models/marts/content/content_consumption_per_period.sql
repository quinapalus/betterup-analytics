WITH

eligible_resources AS (

  SELECT * FROM {{ ref('content_eligible_resources') }}

),

completed_activities AS (

  SELECT *
  FROM {{ ref('stg_app__activities') }}
  WHERE
    completed_at IS NOT NULL
    AND viewed_at IS NOT NULL   -- not all activities marked as complete were viewed

),

activities_with_periods AS (

  SELECT
    date_trunc('month', a.completed_at) AS month,
    date_trunc('week', a.completed_at) AS week,
    resource_id,
    a.activity_id,
    r.language
  FROM completed_activities a
  INNER JOIN eligible_resources r
    USING(resource_id)

),

activities_per_period AS (

  SELECT
    month,
    week,
    resource_id,
    language,
    count(activity_id) AS completed_count
  FROM activities_with_periods
  GROUP BY
    month,
    week,
    resource_id,
    language

)

SELECT *
FROM activities_per_period
ORDER BY
  month nulls last,
  week nulls last,
  completed_count DESC
