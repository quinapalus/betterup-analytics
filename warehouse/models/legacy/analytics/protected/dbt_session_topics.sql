WITH client_notes AS (

  SELECT * FROM {{ ref('stg_app__client_notes') }}

),

development_topics AS (

  SELECT * FROM {{ ref('stg_app__development_topics') }}

),

client_notes_development_topics AS (

  SELECT * FROM {{ ref('stg_app__client_notes_development_topics') }}

),

topic_themes AS (

  SELECT * FROM {{ ref('stg_app__topic_themes') }}

)


SELECT
  cn.session_id,
  cn.member_id,
  cn.creator_id AS coach_id,
  dt.development_topic_id,
  dt.name AS topic,
  t.name AS theme,
  COUNT(*) OVER (PARTITION BY cn.session_id) AS session_topics_count
FROM client_notes AS cn
INNER JOIN client_notes_development_topics AS cn_dt
  ON cn.client_note_id = cn_dt.client_note_id
INNER JOIN development_topics AS dt
  ON cn_dt.development_topic_id = dt.development_topic_id
LEFT OUTER JOIN topic_themes AS t
  ON dt.topic_theme_id = t.topic_theme_id
