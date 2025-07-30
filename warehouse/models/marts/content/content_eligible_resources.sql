WITH

public_learning_content AS (

  SELECT *
  FROM {{ ref('stg_app__resources') }}
  WHERE
    NOT retired
    AND NOT hide_from_recommendation_algorithm -- remove internal/onboarding/out of scope resources
    AND publicly_available
    AND type IN (
      'Resources::LinkResource', 'Resources::IngeniuxResource', 'Resources::ContentResource', 'Resources::LessonResource'
    )
),

completed_activities AS (

  SELECT *
  FROM {{ ref('stg_app__activities') }}
  WHERE
    completed_at IS NOT NULL
    AND viewed_at IS NOT NULL  -- not all activities marked as complete were viewed

),


completed_counts AS (

  SELECT
    count(a.completed_at) AS completed_count,
    resource_id,
    language
  FROM completed_activities a
  INNER JOIN public_learning_content r
    USING(resource_id)
  GROUP BY
    resource_id,
    language

)

SELECT
  completed_count,
  resource_id,
  ROW_NUMBER() OVER (ORDER BY completed_count DESC) AS popularity_rank,
  language
FROM completed_counts
ORDER BY
  completed_count DESC
