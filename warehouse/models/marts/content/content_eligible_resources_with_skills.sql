WITH

eligible_resources AS (

  SELECT * FROM {{ ref('content_eligible_resources') }}

),

app_resource_skills AS (

  SELECT * FROM {{ ref('stg_app__resource_skills') }}

)

SELECT
  completed_count,
  resource_id,
  r.popularity_rank,
  r.language,
  array_compact(array_agg(DISTINCT rs.skill_id) WITHIN GROUP (ORDER BY rs.skill_id ASC)) AS skill_ids
FROM eligible_resources r
LEFT JOIN app_resource_skills rs
  USING(resource_id)
GROUP BY
  completed_count,
  resource_id,
  r.popularity_rank,
  r.language
ORDER BY
  r.popularity_rank,
  resource_id
