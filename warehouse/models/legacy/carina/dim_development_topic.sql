WITH development_topics AS (

  SELECT * FROM {{ ref('stg_app__development_topics') }}

),

topic_themes AS (

  SELECT * FROM {{ ref('stg_app__topic_themes') }}

)

SELECT
  {{ development_topic_key('NULL::int') }} AS development_topic_key,
  'N/A' AS development_topic,
  'N/A' AS development_theme,
  'N/A' AS development_topic_version,
  NULL::int AS app_development_topic_id

UNION ALL

SELECT
  {{ development_topic_key('dt.development_topic_id') }} AS development_topic_key,
  dt.name AS development_topic,
  COALESCE(tt.name, 'N/A') AS development_theme,
  dt.topic_version AS development_topic_version,
  dt.development_topic_id AS app_development_topic_id
FROM development_topics AS dt
-- left join here because 1.0 topics didn't require mapping to theme
LEFT OUTER JOIN topic_themes AS tt
  ON dt.topic_theme_id = tt.topic_theme_id
