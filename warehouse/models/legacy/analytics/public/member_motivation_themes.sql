{{
  config(
    tags=['classification.c3_confidential', 'eu'],
    materialized='table'
  )
}}

WITH assessments AS (

  SELECT * FROM {{ ref("stg_app__assessments") }}

),

new_motivations_classified AS (
  SELECT
    THEME_1 AS topic1,
    THEME_SCORE_1 AS topic1_score,
    THEME_2 AS topic2,
    THEME_SCORE_2 AS topic2_score,
    THEME_3 AS topic3,
    THEME_SCORE_3 AS topic3_score,
    ITEM_RESPONSE AS motivation,
    NOT (
      length(motivation) <= 5 AND
      motivation = regexp_replace(motivation, '[^\x00-\x7F]', '')
    ) AS valid_motivation,
    ASSESSMENT_ID,
    ITEM_KEY as ASSESSMENT_ITEM_KEY,
    SCORED_DATETIME AS scored_at
  FROM {{ ref('assessments__assessment_text_scores') }}
  WHERE item_key IN ('layer2_motivation', 'consumer_motivation')
  QUALIFY 1 = row_number() OVER (
    PARTITION BY ASSESSMENT_ID ORDER BY ITEM_KEY DESC, SCORED_DATETIME DESC
    -- a few assessments feature both motivation item_keys;
    -- in that case we prioritize the most recent scored `layer2_motivation`
  )
)

, submitted_assessments AS (
  SELECT
    assessment_id,
    user_id AS member_id,
    submitted_at
  FROM assessments
  WHERE submitted_at IS NOT null
)

, new_motivations_classified_with_member_id AS (
  SELECT *
  FROM new_motivations_classified
  INNER JOIN submitted_assessments
    USING(assessment_id)
)

SELECT
  member_id,
  motivation,
  valid_motivation,
  ml1.topic_theme_id AS topic_theme1_id,
  ml1.topic_theme_name AS topic_theme1_name,
  topic1,
  topic1_score,
  ml2.topic_theme_id AS topic_theme2_id,
  ml2.topic_theme_name AS topic_theme2_name,
  topic2,
  topic2_score,
  ml3.topic_theme_id AS topic_theme3_id,
  ml3.topic_theme_name AS topic_theme3_name,
  topic3,
  topic3_score,
  assessment_id,
  assessment_item_key,
  submitted_at,
  scored_at
FROM new_motivations_classified_with_member_id mc
LEFT JOIN {{ ref('motivation_labels_mapping') }} ml1
  ON mc.valid_motivation AND mc.topic1 = ml1.model_topic
LEFT JOIN {{ ref('motivation_labels_mapping') }} ml2
  ON mc.valid_motivation AND mc.topic2 = ml2.model_topic
LEFT JOIN {{ ref('motivation_labels_mapping') }} ml3
  ON mc.valid_motivation AND mc.topic3 = ml3.model_topic
