{{
  config(
    tags=['classification.c3_confidential']
  )
}}


WITH member_motivations AS (

  SELECT * FROM {{ ref('fact_qualitative_item_response') }}
  WHERE assessment_item_key = 'motivation'

),

motivation_topics AS (

  SELECT * FROM {{ ref('ml_member_motivation_topics') }}

),

topic_themes AS (

  SELECT * FROM {{ ref('stg_app__topic_themes') }}

),

motivation_themes AS (

  SELECT
    mt.member_key,
    -- create ordered list of themes:
    LISTAGG(tt.name, ',') WITHIN GROUP (ORDER BY mt.topic_theme_rank)
      AS ranked_topic_themes,
    COUNT(mt.topic_theme_id) AS ranked_topic_theme_count
  FROM motivation_topics AS mt
  INNER JOIN topic_themes AS tt
    ON mt.topic_theme_id = tt.topic_theme_id
  WHERE
    -- filter for themes that have relative distance less than 10%
    -- the 10% threshold gives roughly 80% of motivations with 3 or fewer topic themes
    mt.relative_distance <= 0.10
  GROUP BY mt.member_key

),

primary_theme AS (

  SELECT
    mt.member_key,
    tt.name AS primary_topic_theme,
    mt.relative_distance_to_next_topic_theme
      AS relative_distance_to_secondary_topic_theme
  FROM motivation_topics AS mt
  INNER JOIN topic_themes AS tt
    ON mt.topic_theme_id = tt.topic_theme_id
  WHERE
    mt.topic_theme_rank = 1

)


SELECT
  mm.member_key,
  mm.account_key,
  mm.date_key,
  mm.assessment_item_response AS motivation_text,
  mm.assessment_item_response_word_count AS word_count,
  mt.ranked_topic_themes,
  mt.ranked_topic_theme_count,
  pt.primary_topic_theme,
  pt.relative_distance_to_secondary_topic_theme
FROM member_motivations AS mm
-- INNER JOIN here as both of the following tables have one row
-- per motivation:
INNER JOIN motivation_themes AS mt
  ON mm.member_key = mt.member_key
INNER JOIN primary_theme AS pt
  ON mm.member_key = pt.member_key
